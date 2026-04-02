import { useEffect, useState, useCallback, useRef } from 'react';
import { useRouter } from 'next/router';
import Head from 'next/head';
import dynamic from 'next/dynamic';

// D3 force-directed network graph — client-only (SSR-disabled)
const NetworkGraph = dynamic(() => import('../../components/NetworkGraph'), {
  ssr: false,
  loading: () => (
    <div className="flex items-center justify-center h-48 bg-slate-800/30 rounded-lg border border-dashed border-gray-300 text-sm text-slate-400">
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
import { Menu, X, ChevronDown, ChevronUp, ExternalLink, Download, Calendar, TrendingUp, TrendingDown, Minus, AlertTriangle, CheckCircle, Target, Zap, RefreshCw, Clock, BarChart2, Shield, Globe, DollarSign, Search, FileText, Activity, Layers, Users, Mail, Loader2, Check } from 'lucide-react';
import NavHeader from '../../components/NavHeader';

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
  gameplan:             { title: 'The Gameplan',            icon: 'zap',         number: 5  },
  algorithm_impact:     { title: 'Algorithm Impact',        icon: 'shield',      number: 6  },
  intent_migration:     { title: 'Intent Migration',        icon: 'layers',      number: 7  },
  technical_health:     { title: 'CTR Modeling',            icon: 'bar-chart',   number: 8  },
  site_architecture:    { title: 'Site Architecture',       icon: 'globe',       number: 9  },
  branded_split:        { title: 'Branded vs Non-Branded',  icon: 'users',       number: 10 },
  competitive_threats:  { title: 'Competitive Radar',       icon: 'users',       number: 11 },
  revenue_attribution:  { title: 'Revenue Attribution',     icon: 'dollar',      number: 12 },
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
interface ReportData {
  report_id: string;
  domain: string;
  status: string;
  generated_at?: string;
  current_module?: number;
  progress?: Record<string, string>;
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

  // Email modal state
  const [showEmailModal, setShowEmailModal] = useState(false);
  const [emailAddress, setEmailAddress] = useState('');
  const [emailSending, setEmailSending] = useState(false);
  const [emailResult, setEmailResult] = useState<{ success: boolean; message: string } | null>(null);

  // Progressive rendering: show completed modules during generation
  const [progressiveModules, setProgressiveModules] = useState<Record<string, any>>({});
  const progressivePollRef = useRef<NodeJS.Timeout | null>(null);

  const sendReportEmail = async () => {
    if (!emailAddress || !id) return;
    setEmailSending(true);
    setEmailResult(null);
    try {
      const res = await fetch(`${API_BASE}/api/v1/reports/${id}/email`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ to_email: emailAddress }),
      });
      const data = await res.json();
      if (res.ok && data.success) {
        setEmailResult({ success: true, message: data.message || 'Report sent!' });
        setTimeout(() => {
          setShowEmailModal(false);
          setEmailResult(null);
          setEmailAddress('');
        }, 2500);
      } else {
        setEmailResult({ success: false, message: data.detail || data.message || 'Failed to send email' });
      }
    } catch (err: any) {
      setEmailResult({ success: false, message: err.message || 'Network error' });
    } finally {
      setEmailSending(false);
    }
  };

  // ---------------------------------------------------------------------------
  // Progressive module fetching — shows sections as they complete
  // ---------------------------------------------------------------------------
  const fetchProgressiveModules = useCallback(async () => {
    if (!id) return;
    try {
      const res = await fetch(`${API_BASE}/api/v1/reports/${id}/modules`, {
        credentials: 'include',
      });
      if (res.ok) {
        const data = await res.json();
        if (data.modules) {
          const completed: Record<string, any> = {};
          for (const [key, mod] of Object.entries(data.modules)) {
            const m = mod as any;
            if (m.status === 'success' && m.data) {
              completed[key] = m.data;
            }
          }
          setProgressiveModules(completed);
        }
        // Keep polling while report is generating
        if (data.status && !['completed', 'complete', 'done', 'failed', 'partial'].includes(data.status)) {
          progressivePollRef.current = setTimeout(fetchProgressiveModules, 5000);
        }
      }
    } catch {
      // Silent fail — main fetchReport handles errors
    }
  }, [id]);

  // ---------------------------------------------------------------------------
  // Fetch report data from API
  // ---------------------------------------------------------------------------
  const fetchReport = useCallback(async () => {
    if (!id) return;

    try {
      const response = await fetch(`${API_BASE}/api/v1/reports/${id}`, {
        credentials: 'include',
      });
      if (!response.ok) {
        if (response.status === 401) {
          // Auth expired — redirect to home to re-authenticate
          window.location.href = '/?auth=expired';
          return;
        }
        if (response.status === 404) {
          setError('Report not found');
          setLoading(false);
          return;
        }
        throw new Error(`Failed to fetch report: ${response.statusText}`);
      }

      const raw = await response.json();

      // Transform API response (raw Supabase row) to frontend shape.
      // The API returns: { id, domain, status, report_data: { metadata, sections, errors }, ... }
      // Each section has: { status, execution_time_seconds, data: { ... } }
      // The frontend expects a flat modules record: { module_key: module_data }
      const reportData = raw.report_data || {};
      const sections = reportData.sections || {};

      const modules: Record<string, any> = {};
      for (const [key, section] of Object.entries(sections)) {
        if (section && typeof section === 'object') {
          const s = section as any;
          if (s.status === 'success' && s.data) {
            modules[key] = s.data;
          }
        }
      }

      const data: ReportData = {
        report_id: raw.id || (id as string),
        domain: raw.domain || '',
        status: raw.status || 'unknown',
        generated_at: raw.completed_at || raw.created_at,
        current_module: raw.current_module,
        progress: raw.progress,
        summary: reportData.metadata || undefined,
        modules,
      };

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

  // Start progressive module polling during generation
  useEffect(() => {
    if (loading && id) {
      fetchProgressiveModules();
    }
    return () => {
      if (progressivePollRef.current) {
        clearTimeout(progressivePollRef.current);
      }
    };
  }, [loading, id, fetchProgressiveModules]);

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
    const progress = report?.progress || {};
    const completedModules = Object.values(progress).filter(
      (s) => s === 'success' || s === 'completed' || s === 'failed' || s === 'skipped'
    ).length;
    const totalModules = 12;
    const progressPct = totalModules > 0 ? Math.round((completedModules / totalModules) * 100) : 0;
    const currentModuleNum = report?.current_module || 0;
    const currentModuleName = Object.entries(MODULE_META).find(
      ([, meta]) => meta.number === currentModuleNum
    )?.[1]?.title;

    const progressiveKeys = Object.keys(progressiveModules);
    const hasProgressiveData = progressiveKeys.length > 0;

    // Map module keys to their content components for progressive rendering
    const renderProgressiveModule = (key: string, data: any) => {
      const components: Record<string, (props: { data: any; domain?: string }) => JSX.Element | null> = {
        health_trajectory: HealthTrajectoryContent,
        page_triage: PageTriageContent,
        serp_landscape: SerpLandscapeContent,
        content_intelligence: ContentIntelligenceContent,
        gameplan: GameplanContent,
        algorithm_impact: AlgorithmImpactContent,
        intent_migration: IntentMigrationContent,
        technical_health: TechnicalHealthContent,
        site_architecture: SiteArchitectureContent,
        branded_split: BrandedSplitContent,
        competitive_threats: CompetitiveThreatsContent,
        revenue_attribution: RevenueAttributionContent,
      };
      const Comp = components[key];
      return Comp ? <Comp data={data} domain={report?.domain || ''} /> : null;
    };

    return (
      <>
        <Head>
          <title>Generating Report | Search Intelligence</title>
        </Head>
        <NavHeader />
        <main className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 py-12 px-4">
          <div className={hasProgressiveData ? "max-w-7xl mx-auto" : "max-w-4xl mx-auto text-center"}>
            {/* Progress header */}
            <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-8 mb-8">
              <div className="flex items-center justify-center gap-4 mb-4">
                <RefreshCw className="w-8 h-8 text-blue-400 animate-spin" />
                <h1 className="text-2xl font-bold text-white">Generating Your Report</h1>
              </div>
              <p className="text-slate-300 mb-4 text-center">
                {report?.status === 'analyzing' && currentModuleName
                  ? `Running: ${currentModuleName} (${completedModules}/${totalModules} modules)`
                  : report?.status === 'ingesting'
                  ? 'Pulling data from Google Search Console and GA4...'
                  : 'Initializing report generation...'}
              </p>
              {completedModules > 0 && (
                <div className="max-w-md mx-auto mb-2">
                  <div className="w-full bg-slate-700 rounded-full h-3">
                    <div
                      className="bg-gradient-to-r from-blue-500 to-purple-500 h-3 rounded-full transition-all duration-500"
                      style={{ width: `${progressPct}%` }}
                    />
                  </div>
                  <p className="text-sm text-slate-400 mt-2 text-center">{progressPct}% complete</p>
                </div>
              )}
              {report?.domain && (
                <p className="text-sm text-slate-400 text-center">
                  Domain: <span className="font-mono text-slate-300">{report.domain}</span>
                </p>
              )}
            </div>

            {/* Progressive module results — completed sections appear as they finish */}
            {hasProgressiveData && (
              <div className="space-y-6">
                <p className="text-sm text-slate-400 text-center mb-4">
                  Completed sections appear below as they finish analyzing:
                </p>
                {Object.entries(MODULE_META).map(([key, meta]) => {
                  const moduleData = progressiveModules[key];
                  if (!moduleData) return null;
                  return (
                    <div key={key} className="bg-slate-800/40 border border-slate-700/50 rounded-lg overflow-hidden">
                      <button
                        onClick={() => toggleSection(key)}
                        className="w-full flex items-center justify-between p-5 text-left hover:bg-slate-700/30 transition"
                      >
                        <div className="flex items-center gap-3">
                          <div className="w-8 h-8 rounded-full bg-emerald-900/40 flex items-center justify-center">
                            <CheckCircle className="w-4 h-4 text-emerald-400" />
                          </div>
                          <div>
                            <span className="text-white font-medium">{meta.title}</span>
                            <span className="text-xs text-slate-500 ml-2">Module {meta.number}</span>
                          </div>
                        </div>
                        {expandedSections.has(key)
                          ? <ChevronUp className="w-5 h-5 text-slate-400" />
                          : <ChevronDown className="w-5 h-5 text-slate-400" />}
                      </button>
                      {expandedSections.has(key) && (
                        <div className="px-5 pb-5">
                          {renderProgressiveModule(key, moduleData)}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
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
        <main className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 py-12 px-4">
          <div className="max-w-4xl mx-auto text-center">
            <div className="bg-slate-800/50 border border-red-800 rounded-lg p-12">
              <AlertTriangle className="w-16 h-16 text-red-400 mx-auto mb-6" />
              <h1 className="text-2xl font-bold text-white mb-3">
                Unable to Load Report
              </h1>
              <p className="text-slate-300 mb-6">
                {error || 'Report data could not be retrieved'}
              </p>
              <div className="flex flex-col sm:flex-row gap-3 justify-center">
                <button
                  onClick={() => router.push('/')}
                  className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
                >
                  Generate New Report
                </button>
                <button
                  onClick={() => router.push('/reports')}
                  className="px-6 py-3 bg-slate-700 text-white rounded-lg hover:bg-slate-600 transition"
                >
                  My Reports
                </button>
              </div>
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
        <main className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 py-12 px-4">
          <div className="max-w-4xl mx-auto text-center">
            <div className="bg-slate-800/50 border border-yellow-800 rounded-lg p-12">
              <AlertTriangle className="w-16 h-16 text-yellow-400 mx-auto mb-6" />
              <h1 className="text-2xl font-bold text-white mb-3">
                Report Generation Failed
              </h1>
              <p className="text-slate-300 mb-6">
                We encountered an error while generating your report for{' '}
                <span className="font-mono text-slate-200">{report.domain}</span>
              </p>
              <div className="flex flex-col sm:flex-row gap-3 justify-center">
                <button
                  onClick={() => router.push('/')}
                  className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
                >
                  Try Again
                </button>
                <button
                  onClick={() => router.push('/reports')}
                  className="px-6 py-3 bg-slate-700 text-white rounded-lg hover:bg-slate-600 transition"
                >
                  My Reports
                </button>
              </div>
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

      <main className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
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
              <div className="flex items-center space-x-3">
                <button
                  onClick={() => {
                    const a = document.createElement('a');
                    a.href = `${process.env.NEXT_PUBLIC_API_URL || 'https://search-intel-api-production.up.railway.app'}/api/v1/reports/${id}/pdf`;
                    a.download = `search_intelligence_${report.domain}.pdf`;
                    a.click();
                  }}
                  className="flex items-center space-x-2 px-4 py-2 bg-white/10 hover:bg-white/20 rounded-lg transition backdrop-blur-sm"
                >
                  <Download className="w-4 h-4" />
                  <span className="text-sm font-medium">Download PDF</span>
                </button>
                <button
                  onClick={() => setShowEmailModal(true)}
                  className="flex items-center space-x-2 px-4 py-2 bg-white/10 hover:bg-white/20 rounded-lg transition backdrop-blur-sm"
                >
                  <Mail className="w-4 h-4" />
                  <span className="text-sm font-medium">Email Report</span>
                </button>
                <button
                  onClick={() => window.print()}
                  className="flex items-center space-x-2 px-3 py-2 bg-white/10 hover:bg-white/20 rounded-lg transition backdrop-blur-sm text-sm"
                >
                  Print
                </button>
              </div>
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

          {/* Module 5: The Gameplan */}
          {modules.gameplan && (
            <ModuleSection
              moduleKey="gameplan"
              expanded={expandedSections.has('gameplan')}
              onToggle={() => toggleSection('gameplan')}
            >
              <GameplanContent data={modules.gameplan} />
            </ModuleSection>
          )}

          {/* Consulting CTA — after Gameplan (spec requirement) */}
          <div className="my-8 rounded-xl border border-blue-500/30 bg-gradient-to-r from-blue-900/40 via-indigo-900/30 to-blue-900/40 p-6 sm:p-8 text-center">
            <h3 className="text-xl font-semibold text-white mb-2">
              Want help executing this plan?
            </h3>
            <p className="text-slate-300 mb-5 max-w-xl mx-auto">
              Our search strategists turn these recommendations into measurable results.
              Book a free strategy call to discuss your gameplan.
            </p>
            <a
              href="https://clankermarketing.com/book"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-block px-6 py-3 bg-blue-600 hover:bg-blue-500 text-white font-semibold rounded-lg transition shadow-lg shadow-blue-600/20"
            >
              Book a Strategy Call
            </a>
            <p className="text-slate-500 text-xs mt-4">
              Clanker Marketing — Search Intelligence Consulting
            </p>
          </div>

          {/* Module 6: Algorithm Impact */}
          {modules.algorithm_impact && (
            <ModuleSection
              moduleKey="algorithm_impact"
              expanded={expandedSections.has('algorithm_impact')}
              onToggle={() => toggleSection('algorithm_impact')}
            >
              <AlgorithmImpactContent data={modules.algorithm_impact} />
            </ModuleSection>
          )}

          {/* Module 7: Intent Migration */}
          {modules.intent_migration && (
            <ModuleSection
              moduleKey="intent_migration"
              expanded={expandedSections.has('intent_migration')}
              onToggle={() => toggleSection('intent_migration')}
            >
              <IntentMigrationContent data={modules.intent_migration} />
            </ModuleSection>
          )}

          {/* Module 8: CTR Modeling */}
          {modules.technical_health && (
            <ModuleSection
              moduleKey="technical_health"
              expanded={expandedSections.has('technical_health')}
              onToggle={() => toggleSection('technical_health')}
            >
              <TechnicalHealthContent data={modules.technical_health} />
            </ModuleSection>
          )}

          {/* Module 9: Site Architecture */}
          {modules.site_architecture && (
            <ModuleSection
              moduleKey="site_architecture"
              expanded={expandedSections.has('site_architecture')}
              onToggle={() => toggleSection('site_architecture')}
            >
              <SiteArchitectureContent data={modules.site_architecture} />
            </ModuleSection>
          )}

          {/* Module 10: Branded vs Non-Branded */}
          {modules.branded_split && (
            <ModuleSection
              moduleKey="branded_split"
              expanded={expandedSections.has('branded_split')}
              onToggle={() => toggleSection('branded_split')}
            >
              <BrandedSplitContent data={modules.branded_split} />
            </ModuleSection>
          )}

          {/* Module 11: Competitive Radar */}
          {modules.competitive_threats && (
            <ModuleSection
              moduleKey="competitive_threats"
              expanded={expandedSections.has('competitive_threats')}
              onToggle={() => toggleSection('competitive_threats')}
            >
              <CompetitiveThreatsContent data={modules.competitive_threats} />
            </ModuleSection>
          )}

          {/* Module 12: Revenue Attribution */}
          {modules.revenue_attribution && (
            <ModuleSection
              moduleKey="revenue_attribution"
              expanded={expandedSections.has('revenue_attribution')}
              onToggle={() => toggleSection('revenue_attribution')}
            >
              <RevenueAttributionContent data={modules.revenue_attribution} />
            </ModuleSection>
          )}

          {/* Consulting CTA — after Revenue Attribution (spec requirement) */}
          {modules.revenue_attribution && (
            <div className="my-8 rounded-xl border border-emerald-500/30 bg-gradient-to-r from-emerald-900/40 via-teal-900/30 to-emerald-900/40 p-6 sm:p-8 text-center">
              <h3 className="text-xl font-semibold text-white mb-2">
                These opportunities total{' '}
                <span className="text-emerald-400">
                  {modules.revenue_attribution?.total_potential_value
                    ? `$${Math.round(modules.revenue_attribution.total_potential_value).toLocaleString()}/month`
                    : 'significant revenue'}
                </span>
              </h3>
              <p className="text-slate-300 mb-5 max-w-xl mx-auto">
                Let&apos;s capture them together. Our team specialises in turning search
                intelligence into revenue growth.
              </p>
              <a
                href="https://clankermarketing.com/book"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-block px-6 py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-semibold rounded-lg transition shadow-lg shadow-emerald-600/20"
              >
                Let&apos;s Capture This Revenue
              </a>
              <p className="text-slate-500 text-xs mt-4">
                Clanker Marketing — Search Intelligence Consulting
              </p>
            </div>
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
            <a
              href="https://clankermarketing.com/book"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-block px-8 py-4 bg-white/20 hover:bg-white/30 text-white rounded-lg font-semibold transition text-lg shadow-lg"
            >
              Work With Us
            </a>
            <p className="text-blue-200/60 text-sm mt-6">
              Powered by <a href="https://clankermarketing.com" target="_blank" rel="noopener noreferrer" className="underline hover:text-white transition">Clanker Marketing</a> — Search Intelligence Consulting
            </p>
          </div>
        </div>

        {/* Email Report Modal */}
        {showEmailModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm" onClick={() => { if (!emailSending) { setShowEmailModal(false); setEmailResult(null); } }}>
            <div className="bg-slate-800 border border-slate-700/50 rounded-2xl shadow-2xl w-full max-w-md mx-4 p-6" onClick={(e) => e.stopPropagation()}>
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center space-x-3">
                  <div className="w-10 h-10 bg-blue-600/20 rounded-lg flex items-center justify-center">
                    <Mail className="w-5 h-5 text-blue-400" />
                  </div>
                  <div>
                    <h3 className="text-lg font-semibold text-white">Email Report</h3>
                    <p className="text-sm text-slate-400">Send a PDF copy via email</p>
                  </div>
                </div>
                <button
                  onClick={() => { if (!emailSending) { setShowEmailModal(false); setEmailResult(null); } }}
                  className="p-1 rounded-lg text-slate-400 hover:text-white hover:bg-slate-700/50 transition"
                >
                  <X className="w-5 h-5" />
                </button>
              </div>

              {emailResult ? (
                <div className={`flex items-center space-x-3 p-4 rounded-lg ${emailResult.success ? 'bg-emerald-900/30 border border-emerald-700/50' : 'bg-red-900/30 border border-red-700/50'}`}>
                  {emailResult.success ? (
                    <Check className="w-5 h-5 text-emerald-400 flex-shrink-0" />
                  ) : (
                    <AlertTriangle className="w-5 h-5 text-red-400 flex-shrink-0" />
                  )}
                  <p className={`text-sm ${emailResult.success ? 'text-emerald-300' : 'text-red-300'}`}>
                    {emailResult.message}
                  </p>
                </div>
              ) : (
                <>
                  <div className="mb-4">
                    <label htmlFor="email-input" className="block text-sm font-medium text-slate-300 mb-2">
                      Recipient email address
                    </label>
                    <input
                      id="email-input"
                      type="email"
                      value={emailAddress}
                      onChange={(e) => setEmailAddress(e.target.value)}
                      onKeyDown={(e) => { if (e.key === 'Enter' && emailAddress) sendReportEmail(); }}
                      placeholder="name@company.com"
                      disabled={emailSending}
                      className="w-full px-4 py-2.5 bg-slate-700/60 border border-slate-600 rounded-lg text-white placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent disabled:opacity-50 text-sm"
                      autoFocus
                    />
                  </div>
                  <div className="flex items-center justify-end space-x-3">
                    <button
                      onClick={() => { setShowEmailModal(false); setEmailResult(null); }}
                      disabled={emailSending}
                      className="px-4 py-2 text-sm text-slate-300 hover:text-white transition disabled:opacity-50"
                    >
                      Cancel
                    </button>
                    <button
                      onClick={sendReportEmail}
                      disabled={emailSending || !emailAddress}
                      className="flex items-center space-x-2 px-5 py-2.5 bg-blue-600 hover:bg-blue-500 disabled:bg-blue-600/50 text-white text-sm font-medium rounded-lg transition disabled:cursor-not-allowed"
                    >
                      {emailSending ? (
                        <>
                          <Loader2 className="w-4 h-4 animate-spin" />
                          <span>Sending...</span>
                        </>
                      ) : (
                        <>
                          <Mail className="w-4 h-4" />
                          <span>Send Report</span>
                        </>
                      )}
                    </button>
                  </div>
                </>
              )}
            </div>
          </div>
        )}

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
    <div className="bg-slate-800/60 rounded-lg shadow-sm border border-slate-700/50 overflow-hidden">
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between p-6 hover:bg-slate-800/30 transition text-left"
      >
        <div className="flex items-center space-x-4">
          <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center text-blue-400 flex-shrink-0">
            <IconComponent className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center space-x-2">
              <span className="text-xs font-semibold text-blue-400 uppercase tracking-wide">
                Module {meta.number}
              </span>
            </div>
            <h2 className="text-xl font-bold text-white mt-1">{meta.title}</h2>
          </div>
        </div>
        <div className="text-slate-500">
          {expanded ? (
            <ChevronUp className="w-6 h-6" />
          ) : (
            <ChevronDown className="w-6 h-6" />
          )}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-slate-700/50 p-6 bg-slate-800/30">{children}</div>
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
  if (!data) return <div className="text-slate-400">No data available</div>;

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
              <TrendingUp className="w-5 h-5 text-emerald-400" />
            ) : trendDirection === 'declining' || trendDirection === 'decline' ? (
              <TrendingDown className="w-5 h-5 text-red-400" />
            ) : (
              <Minus className="w-5 h-5 text-slate-400" />
            )
          }
        />
        <MetricCard
          label="Monthly Change"
          value={`${trendSlope >= 0 ? '+' : ''}${trendSlope.toFixed(1)}%`}
          className={trendSlope >= 0 ? 'text-emerald-400' : 'text-red-400'}
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
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
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
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Detected Change Points
          </h3>
          <div className="space-y-2">
            {data.change_points.map((cp: any, idx: number) => (
              <div
                key={idx}
                className="flex items-center justify-between p-3 bg-slate-800/30 rounded"
              >
                <div>
                  <div className="font-medium text-white">{cp.date}</div>
                  <div className="text-sm text-slate-400 capitalize">
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
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Seasonality Patterns
          </h3>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <div className="text-sm text-slate-400">Best Day</div>
              <div className="text-lg font-semibold text-white">
                {data.seasonality.best_day || 'N/A'}
              </div>
            </div>
            <div>
              <div className="text-sm text-slate-400">Worst Day</div>
              <div className="text-lg font-semibold text-white">
                {data.seasonality.worst_day || 'N/A'}
              </div>
            </div>
          </div>
          {data.seasonality.cycle_description && (
            <p className="text-sm text-slate-400 mt-3">
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
  if (!data) return <div className="text-slate-400">No data available</div>;

  const pages = data.pages || [];
  const summary = data.summary || {};

  return (
    <div className="space-y-6">
      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <MetricCard label="Growing" value={summary.growing || 0} className="text-emerald-400" />
        <MetricCard label="Stable" value={summary.stable || 0} className="text-slate-400" />
        <MetricCard label="Decaying" value={summary.decaying || 0} className="text-amber-400" />
        <MetricCard label="Critical" value={summary.critical || 0} className="text-red-400" />
      </div>

      {/* Pages Table */}
      <div className="bg-slate-800/60 rounded-lg border border-slate-700/50 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-slate-800/30 border-b border-slate-700/50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-semibold text-slate-300 uppercase">
                  Page
                </th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-slate-300 uppercase">
                  Status
                </th>
                <th className="px-4 py-3 text-right text-xs font-semibold text-slate-300 uppercase">
                  Monthly Clicks
                </th>
                <th className="px-4 py-3 text-right text-xs font-semibold text-slate-300 uppercase">
                  Trend
                </th>
                <th className="px-4 py-3 text-right text-xs font-semibold text-slate-300 uppercase">
                  Priority
                </th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-slate-300 uppercase">
                  Action
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/50">
              {pages.slice(0, 20).map((page: any, idx: number) => (
                <tr key={idx} className="hover:bg-slate-800/80">
                  <td className="px-4 py-3">
                    <a
                      href={page.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-sm text-blue-400 hover:underline flex items-center"
                    >
                      <span className="truncate max-w-xs">{page.url}</span>
                      <ExternalLink className="w-3 h-3 ml-1 flex-shrink-0" />
                    </a>
                  </td>
                  <td className="px-4 py-3">
                    <StatusBadge status={page.bucket} />
                  </td>
                  <td className="px-4 py-3 text-right text-sm text-white">
                    {page.current_monthly_clicks?.toLocaleString() || 0}
                  </td>
                  <td className="px-4 py-3 text-right text-sm">
                    <span
                      className={
                        page.trend_slope >= 0 ? 'text-emerald-400' : 'text-red-400'
                      }
                    >
                      {page.trend_slope >= 0 ? '+' : ''}
                      {page.trend_slope?.toFixed(2) || 0}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-right">
                    <PriorityBadge score={page.priority_score} />
                  </td>
                  <td className="px-4 py-3 text-sm text-slate-300">
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
  if (!data) return <div className="text-slate-400">No data available</div>;

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
          value={`${((data.click_share?.total_click_share || 0) * 100).toFixed(1)}%`}
        />
        <MetricCard
          label="Opportunity"
          value={`${((data.click_share?.click_opportunity || 0) * 100).toFixed(1)}%`}
        />
      </div>

      {/* SERP Feature Displacement */}
      {displacements.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            SERP Feature Displacement
          </h3>
          <div className="space-y-3">
            {displacements.slice(0, 10).map((disp: any, idx: number) => (
              <div
                key={idx}
                className="flex items-start justify-between p-3 bg-slate-800/30 rounded"
              >
                <div className="flex-1">
                  <div className="font-medium text-white">{disp.keyword}</div>
                  <div className="text-sm text-slate-400 mt-1">
                    Organic #{disp.organic_position} → Visual #{disp.visual_position}
                  </div>
                  <div className="text-xs text-slate-400 mt-1">
                    {disp.features_above?.join(', ')}
                  </div>
                </div>
                <div className="text-right ml-4">
                  <div className="text-sm font-medium text-red-400">
                    {((disp.estimated_ctr_impact || 0) * 100).toFixed(1)}%
                  </div>
                  <div className="text-xs text-slate-400">CTR impact</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Competitors */}
      {competitors.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Top Competitors
          </h3>
          <div className="space-y-2">
            {competitors.slice(0, 10).map((comp: any, idx: number) => (
              <div
                key={idx}
                className="flex items-center justify-between p-3 bg-slate-800/30 rounded"
              >
                <div>
                  <div className="font-medium text-white">{comp.domain}</div>
                  <div className="text-sm text-slate-400">
                    {comp.keywords_shared} shared keywords
                  </div>
                </div>
                <div className="text-right">
                  <div className="text-sm font-medium text-white">
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
  if (!data) return <div className="text-slate-400">No data available</div>;

  const cannibalization = data.cannibalization_clusters || [];
  const strikingDistance = data.striking_distance || [];

  return (
    <div className="space-y-6">
      {/* Cannibalization */}
      {cannibalization.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Keyword Cannibalization
          </h3>
          <div className="space-y-3">
            {cannibalization.map((cluster: any, idx: number) => (
              <div key={idx} className="p-3 bg-amber-900/30 border border-yellow-200 rounded">
                <div className="font-medium text-white mb-2">
                  {cluster.query_group}
                </div>
                <div className="text-sm text-slate-300 space-y-1">
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
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Striking Distance Opportunities
          </h3>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-slate-800/30 border-b border-slate-700/50">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-slate-300 uppercase">
                    Query
                  </th>
                  <th className="px-4 py-3 text-right text-xs font-semibold text-slate-300 uppercase">
                    Position
                  </th>
                  <th className="px-4 py-3 text-right text-xs font-semibold text-slate-300 uppercase">
                    Impressions
                  </th>
                  <th className="px-4 py-3 text-right text-xs font-semibold text-slate-300 uppercase">
                    Click Gain
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-slate-300 uppercase">
                    Intent
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700/50">
                {strikingDistance.slice(0, 15).map((opp: any, idx: number) => (
                  <tr key={idx} className="hover:bg-slate-800/80">
                    <td className="px-4 py-3 text-sm text-white">{opp.query}</td>
                    <td className="px-4 py-3 text-right text-sm text-white">
                      {opp.current_position?.toFixed(1)}
                    </td>
                    <td className="px-4 py-3 text-right text-sm text-white">
                      {opp.impressions?.toLocaleString()}
                    </td>
                    <td className="px-4 py-3 text-right text-sm text-emerald-400 font-medium">
                      +{opp.estimated_click_gain_if_top5?.toLocaleString()}
                    </td>
                    <td className="px-4 py-3 text-sm text-slate-300 capitalize">
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
  if (!data) return <div className="text-slate-400">No data available</div>;

  return (
    <div className="space-y-6">
      {/* Summary */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <MetricCard
          label="Recovery Potential"
          value={`${data.total_estimated_monthly_click_recovery?.toLocaleString() || 0} clicks/mo`}
          className="text-emerald-400"
        />
        <MetricCard
          label="Growth Opportunity"
          value={`${data.total_estimated_monthly_click_growth?.toLocaleString() || 0} clicks/mo`}
          className="text-blue-400"
        />
      </div>

      {/* Narrative */}
      {data.narrative && (
        <div className="bg-blue-900/30 border border-blue-200 rounded-lg p-4">
          <p className="text-sm text-slate-100 leading-relaxed whitespace-pre-wrap">
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
    red: 'bg-red-900/30 border-red-200',
    yellow: 'bg-amber-900/30 border-yellow-200',
    blue: 'bg-blue-900/30 border-blue-200',
    gray: 'bg-slate-800/30 border-slate-700/50',
  };

  return (
    <div className={`rounded-lg border p-4 ${colorClasses[color as keyof typeof colorClasses]}`}>
      <h3 className="text-sm font-semibold text-white mb-3">{title}</h3>
      <div className="space-y-2">
        {actions.map((action: any, idx: number) => (
          <div key={idx} className="bg-slate-800/60 p-3 rounded border border-slate-700/50">
            <div className="font-medium text-white">{action.action}</div>
            <div className="grid grid-cols-3 gap-2 mt-2 text-xs text-slate-400">
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
  if (!data) return <div className="text-slate-400">No data available</div>;

  const updates = data.updates_impacting_site || [];

  return (
    <div className="space-y-6">
      <MetricCard
        label="Vulnerability Score"
        value={`${((data.vulnerability_score || 0) * 100).toFixed(0)}%`}
        className={
          (data.vulnerability_score || 0) > 0.7
            ? 'text-red-400'
            : (data.vulnerability_score || 0) > 0.4
            ? 'text-amber-400'
            : 'text-emerald-400'
        }
      />

      {updates.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Algorithm Updates Impact
          </h3>
          <div className="space-y-3">
            {updates.map((update: any, idx: number) => (
              <div
                key={idx}
                className={`p-3 rounded border ${
                  update.site_impact === 'negative'
                    ? 'bg-red-900/30 border-red-200'
                    : update.site_impact === 'positive'
                    ? 'bg-emerald-900/30 border-green-200'
                    : 'bg-slate-800/30 border-slate-700/50'
                }`}
              >
                <div className="flex items-start justify-between">
                  <div>
                    <div className="font-medium text-white">
                      {update.update_name}
                    </div>
                    <div className="text-sm text-slate-400 mt-1">{update.date}</div>
                    {update.common_characteristics && (
                      <div className="text-xs text-slate-400 mt-2">
                        Affected: {update.common_characteristics.join(', ')}
                      </div>
                    )}
                  </div>
                  <div className="text-right ml-4">
                    <div
                      className={`text-sm font-medium ${
                        update.click_change_pct >= 0
                          ? 'text-emerald-400'
                          : 'text-red-400'
                      }`}
                    >
                      {update.click_change_pct >= 0 ? '+' : ''}
                      {update.click_change_pct?.toFixed(1)}%
                    </div>
                    <div className="text-xs text-slate-400 mt-1">
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
        <div className="bg-blue-900/30 border border-blue-200 rounded-lg p-4">
          <p className="text-sm text-slate-100">{data.recommendation}</p>
        </div>
      )}
    </div>
  );
}

// Module 8: Intent Migration
function IntentMigrationContent({ data }: { data: any }) {
  if (!data) return <div className="text-slate-400">No data available</div>;

  const shifts = data.intent_shifts || [];
  const emerging = data.emerging_intents || [];
  const portfolio = data.portfolio_distribution || {};

  return (
    <div className="space-y-6">
      {/* Portfolio Distribution */}
      {portfolio && Object.keys(portfolio).length > 0 && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {Object.entries(portfolio).map(([intent, pct]: [string, any]) => (
            <div key={intent} className="bg-slate-800/60 p-3 rounded-lg border border-slate-700/50 text-center">
              <div className="text-xs text-slate-400 capitalize">{intent}</div>
              <div className="text-xl font-bold text-white mt-1">
                {typeof pct === 'number' ? `${(pct * 100).toFixed(0)}%` : String(pct)}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Intent Shifts */}
      {shifts.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Detected Intent Shifts ({shifts.length})
          </h3>
          <div className="space-y-3">
            {shifts.slice(0, 15).map((shift: any, idx: number) => (
              <div
                key={idx}
                className="p-3 bg-purple-900/30 border border-purple-700/40 rounded"
              >
                <div className="font-medium text-white">{shift.query || shift.keyword}</div>
                <div className="text-sm text-slate-300 mt-2">
                  {shift.previous_intent || shift.from_intent} → {shift.current_intent || shift.to_intent}
                </div>
                {shift.confidence != null && (
                  <div className="text-xs text-slate-400 mt-1">
                    Confidence: {((shift.confidence || 0) * 100).toFixed(0)}%
                  </div>
                )}
                {shift.recommendation && (
                  <div className="text-sm text-purple-300 mt-2 font-medium">
                    → {shift.recommendation}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Emerging Intents */}
      {emerging.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Emerging Intents ({emerging.length})
          </h3>
          <div className="space-y-2">
            {emerging.slice(0, 10).map((item: any, idx: number) => (
              <div key={idx} className="flex justify-between p-2 bg-slate-800/30 rounded">
                <span className="text-white text-sm">{item.query || item.keyword}</span>
                <span className="text-emerald-400 text-sm font-medium">{item.intent || item.emerging_intent}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recommendations */}
      {data.recommendations?.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-3">Recommendations</h3>
          <ul className="space-y-2">
            {data.recommendations.slice(0, 5).map((rec: any, idx: number) => (
              <li key={idx} className="text-sm text-slate-300 flex items-start gap-2">
                <span className="text-blue-400 mt-0.5">•</span>
                <span>{typeof rec === 'string' ? rec : rec.text || rec.recommendation || JSON.stringify(rec)}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

// Module 9: Technical Health (CTR Modeling)
function TechnicalHealthContent({ data }: { data: any }) {
  if (!data) return <div className="text-slate-400">No data available</div>;

  const keywordAnalysis = data.keyword_ctr_analysis || [];
  const opportunities = data.feature_opportunities || data.serp_feature_opportunities || [];

  // Prepare scatter data: expected vs actual CTR
  const scatterData = keywordAnalysis.slice(0, 50).map((kw: any) => ({
    keyword: kw.keyword,
    expected: parseFloat(((kw.expected_ctr_contextual || kw.expected_ctr_generic || 0) * 100).toFixed(2)),
    actual: parseFloat(((kw.actual_ctr || 0) * 100).toFixed(2)),
    impressions: kw.impressions || 0,
    position: kw.position || 0,
    performance: (kw.actual_ctr || 0) > (kw.expected_ctr_contextual || kw.expected_ctr_generic || 0)
      ? 'overperformer' : 'underperformer',
  }));

  const overperformers = keywordAnalysis.filter(
    (kw: any) => (kw.actual_ctr || 0) > (kw.expected_ctr_contextual || kw.expected_ctr_generic || 0)
  );
  const underperformers = keywordAnalysis.filter(
    (kw: any) => (kw.actual_ctr || 0) <= (kw.expected_ctr_contextual || kw.expected_ctr_generic || 0)
  );

  return (
    <div className="space-y-6">
      {/* Summary Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <MetricCard
          label="Model Accuracy (R\u00b2)"
          value={data.ctr_model_accuracy ? `${(data.ctr_model_accuracy * 100).toFixed(0)}%` : 'N/A'}
        />
        <MetricCard
          label="Overperformers"
          value={overperformers.length}
          className="text-emerald-400"
        />
        <MetricCard
          label="Underperformers"
          value={underperformers.length}
          className="text-red-400"
        />
      </div>

      {/* CTR Scatter Plot: Expected vs Actual */}
      {scatterData.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Expected vs Actual CTR
          </h3>
          <p className="text-xs text-slate-400 mb-3">
            Points above the diagonal are overperforming; below are underperforming relative to SERP context.
          </p>
          <ResponsiveContainer width="100%" height={350}>
            <ScatterChart margin={{ top: 10, right: 20, bottom: 20, left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                type="number"
                dataKey="expected"
                name="Expected CTR"
                tick={{ fontSize: 11 }}
                label={{ value: 'Expected CTR (%)', position: 'insideBottom', offset: -10, fontSize: 12 }}
              />
              <YAxis
                type="number"
                dataKey="actual"
                name="Actual CTR"
                tick={{ fontSize: 11 }}
                label={{ value: 'Actual CTR (%)', angle: -90, position: 'insideLeft', offset: 0, fontSize: 12 }}
              />
              <Tooltip
                content={({ active, payload }: any) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return (
                    <div className="bg-slate-800/60 p-3 rounded shadow-lg border text-xs">
                      <div className="font-semibold text-white mb-1">{d.keyword}</div>
                      <div>Position: #{d.position}</div>
                      <div>Expected CTR: {d.expected}%</div>
                      <div>Actual CTR: {d.actual}%</div>
                      <div>Impressions: {d.impressions.toLocaleString()}</div>
                    </div>
                  );
                }}
              />
              <Scatter data={scatterData} fill="#3b82f6">
                {scatterData.map((entry: any, index: number) => (
                  <Cell
                    key={index}
                    fill={entry.performance === 'overperformer' ? '#22c55e' : '#ef4444'}
                  />
                ))}
              </Scatter>
              <ReferenceLine
                segment={[{ x: 0, y: 0 }, { x: Math.max(...scatterData.map((d: any) => Math.max(d.expected, d.actual)), 10), y: Math.max(...scatterData.map((d: any) => Math.max(d.expected, d.actual)), 10) }]}
                stroke="#94a3b8"
                strokeDasharray="5 5"
                label=""
              />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Top Underperformers — Title Rewrite Candidates */}
      {underperformers.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Title/Snippet Rewrite Candidates
          </h3>
          <p className="text-xs text-slate-400 mb-3">
            Keywords where actual CTR is significantly below what the SERP context predicts — likely a title or meta description problem.
          </p>
          <div className="space-y-2">
            {underperformers
              .sort((a: any, b: any) => {
                const gapA = (a.expected_ctr_contextual || a.expected_ctr_generic || 0) - (a.actual_ctr || 0);
                const gapB = (b.expected_ctr_contextual || b.expected_ctr_generic || 0) - (b.actual_ctr || 0);
                return gapB - gapA;
              })
              .slice(0, 10)
              .map((kw: any, idx: number) => {
                const expected = kw.expected_ctr_contextual || kw.expected_ctr_generic || 0;
                const gap = expected - (kw.actual_ctr || 0);
                return (
                  <div key={idx} className="flex items-center justify-between p-3 bg-red-900/30 border border-red-100 rounded">
                    <div className="flex-1 min-w-0">
                      <div className="font-medium text-white text-sm truncate">{kw.keyword}</div>
                      <div className="text-xs text-slate-400 mt-1">
                        Position #{kw.position} · {(kw.impressions || 0).toLocaleString()} impressions/mo
                      </div>
                    </div>
                    <div className="text-right ml-4 flex-shrink-0">
                      <div className="text-sm text-red-400 font-medium">
                        -{(gap * 100).toFixed(1)}% CTR gap
                      </div>
                      <div className="text-xs text-slate-400">
                        {((kw.actual_ctr || 0) * 100).toFixed(1)}% actual vs {(expected * 100).toFixed(1)}% expected
                      </div>
                    </div>
                  </div>
                );
              })}
          </div>
        </div>
      )}

      {/* SERP Feature Opportunities */}
      {opportunities.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            SERP Feature Opportunities
          </h3>
          <div className="space-y-2">
            {opportunities.slice(0, 8).map((opp: any, idx: number) => (
              <div key={idx} className="flex items-center justify-between p-3 bg-emerald-900/30 border border-green-100 rounded">
                <div className="flex-1 min-w-0">
                  <div className="font-medium text-white text-sm truncate">{opp.keyword}</div>
                  <div className="text-xs text-slate-400 mt-1">{opp.feature_type || opp.opportunity}</div>
                </div>
                <div className="text-right ml-4 flex-shrink-0">
                  <div className="text-sm text-emerald-400 font-medium">
                    +{(opp.estimated_click_gain || 0).toLocaleString()} clicks/mo
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// Module 10: Site Architecture
function SiteArchitectureContent({ data }: { data: any }) {
  if (!data) return <div className="text-slate-400">No data available</div>;

  const hubs = data.hub_pages || [];
  const orphans = data.orphan_pages || [];

  return (
    <div className="space-y-6">
      {/* Network Graph */}
      {data.network_graph && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Internal Link Network
          </h3>
          <NetworkGraph data={data.network_graph} />
        </div>
      )}

      {/* Hub Pages */}
      {hubs.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Hub Pages (High PageRank)
          </h3>
          <div className="space-y-2">
            {hubs.slice(0, 10).map((hub: any, idx: number) => (
              <div
                key={idx}
                className="flex items-center justify-between p-3 bg-slate-800/30 rounded"
              >
                <a
                  href={hub.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-sm text-blue-400 hover:underline flex items-center truncate flex-1"
                >
                  <span className="truncate">{hub.url}</span>
                  <ExternalLink className="w-3 h-3 ml-1 flex-shrink-0" />
                </a>
                <div className="ml-4 text-right">
                  <div className="text-sm font-medium text-white">
                    PR: {hub.pagerank?.toFixed(3)}
                  </div>
                  <div className="text-xs text-slate-400">
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
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Orphan Pages (No Internal Links)
          </h3>
          <div className="space-y-2">
            {orphans.slice(0, 10).map((orphan: any, idx: number) => (
              <div key={idx} className="p-3 bg-amber-900/30 border border-yellow-200 rounded">
                <a
                  href={orphan}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-sm text-blue-400 hover:underline flex items-center"
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
  if (!data) return <div className="text-slate-400">No data available</div>;

  // Module 10 returns: segments.branded, segments.non_branded (aggregates),
  // branded_pct (float 0-100), brand_dependency (object), trends, etc.
  const branded = data.segments?.branded || {};
  const nonBranded = data.segments?.non_branded || {};
  const dependency = data.brand_dependency || {};
  const brandedPct = data.branded_pct ?? dependency.branded_click_share_pct ?? 0;
  const nbGrowth = data.non_branded_growth;
  const topBranded = data.top_branded_queries || [];
  const topNonBranded = data.top_non_branded_queries || [];
  const opportunities = data.non_branded_opportunities || [];

  return (
    <div className="space-y-6">
      {/* Summary Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-2">
            Branded Traffic
          </h3>
          <div className="text-3xl font-bold text-white">
            {(branded.clicks || branded.total_clicks || 0).toLocaleString()}
          </div>
          <div className="text-sm text-slate-400 mt-1">clicks/month</div>
        </div>
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-2">
            Non-Branded Traffic
          </h3>
          <div className="text-3xl font-bold text-white">
            {(nonBranded.clicks || nonBranded.total_clicks || 0).toLocaleString()}
          </div>
          <div className="text-sm text-slate-400 mt-1">clicks/month</div>
        </div>
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-2">
            Brand Dependency
          </h3>
          <div className="text-2xl font-bold text-white">
            {brandedPct > 1 ? brandedPct.toFixed(1) : (brandedPct * 100).toFixed(1)}%
          </div>
          <p className="text-xs text-slate-400 mt-2">
            {(brandedPct > 1 ? brandedPct : brandedPct * 100) > 70
              ? 'High dependency — diversify with non-branded content'
              : (brandedPct > 1 ? brandedPct : brandedPct * 100) > 40
              ? 'Moderate dependency — healthy balance'
              : 'Strong non-branded presence — good diversification'}
          </p>
        </div>
      </div>

      {/* Non-Branded Growth */}
      {nbGrowth != null && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-2">
            Non-Branded Growth Trend
          </h3>
          <div className={`text-2xl font-bold ${nbGrowth > 0 ? 'text-emerald-400' : nbGrowth < 0 ? 'text-red-400' : 'text-white'}`}>
            {nbGrowth > 0 ? '+' : ''}{typeof nbGrowth === 'number' ? nbGrowth.toFixed(1) : nbGrowth}%
          </div>
        </div>
      )}

      {/* Top Non-Branded Queries */}
      {topNonBranded.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-3">
            Top Non-Branded Queries
          </h3>
          <div className="space-y-2">
            {topNonBranded.slice(0, 10).map((q: any, idx: number) => (
              <div key={idx} className="flex justify-between p-2 bg-slate-800/30 rounded text-sm">
                <span className="text-white truncate mr-4">{q.query || q.keyword}</span>
                <span className="text-slate-400 whitespace-nowrap">{(q.clicks || 0).toLocaleString()} clicks</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Opportunities */}
      {opportunities.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-3">
            Non-Branded Opportunities ({opportunities.length})
          </h3>
          <div className="space-y-2">
            {opportunities.slice(0, 8).map((opp: any, idx: number) => (
              <div key={idx} className="p-2 bg-emerald-900/20 border border-emerald-700/30 rounded text-sm">
                <div className="text-white font-medium">{opp.query || opp.keyword}</div>
                {opp.potential_clicks && (
                  <div className="text-xs text-emerald-400 mt-1">+{opp.potential_clicks.toLocaleString()} potential clicks</div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recommendations */}
      {data.recommendations?.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-3">Recommendations</h3>
          <ul className="space-y-2">
            {data.recommendations.slice(0, 5).map((rec: any, idx: number) => (
              <li key={idx} className="text-sm text-slate-300 flex items-start gap-2">
                <span className="text-blue-400 mt-0.5">•</span>
                <span>{typeof rec === 'string' ? rec : rec.text || rec.recommendation || JSON.stringify(rec)}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

// Module 12: Competitive Threats
function CompetitiveThreatsContent({ data }: { data: any }) {
  if (!data) return <div className="text-slate-400">No data available</div>;

  // Module 11 returns: competitor_profiles, keyword_vulnerability,
  // emerging_threats, content_velocity, competitive_pressure, recommendations
  const competitors = data.competitor_profiles || [];
  const vulnerability = data.keyword_vulnerability || {};
  const emerging = data.emerging_threats || [];
  const pressure = data.competitive_pressure || [];

  return (
    <div className="space-y-6">
      {/* Summary */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <MetricCard
          label="Keywords Analyzed"
          value={data.keywords_analyzed || 0}
        />
        <MetricCard
          label="Vulnerable Keywords"
          value={vulnerability.total_vulnerable || 0}
          className="text-red-400"
        />
        <MetricCard
          label="Competitors Found"
          value={competitors.length}
        />
      </div>

      {/* Competitor Profiles */}
      {competitors.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Top Competitors ({competitors.length})
          </h3>
          <div className="space-y-3">
            {competitors.slice(0, 10).map((comp: any, idx: number) => (
              <div
                key={idx}
                className="p-3 bg-slate-800/30 border border-slate-700/40 rounded"
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="font-medium text-white">
                      {comp.domain || comp.competitor}
                    </div>
                    <div className="text-sm text-slate-300 mt-1">
                      {comp.keyword_overlap || comp.shared_keywords || 0} shared keywords
                      {comp.avg_position && (
                        <span className="text-slate-400 ml-2">• Avg pos: #{comp.avg_position.toFixed(1)}</span>
                      )}
                    </div>
                    {comp.threat_score != null && (
                      <div className="text-xs text-slate-400 mt-1">
                        Threat score: {(comp.threat_score * 100).toFixed(0)}%
                      </div>
                    )}
                  </div>
                  <ThreatBadge level={comp.threat_level || (comp.threat_score > 0.7 ? 'high' : comp.threat_score > 0.4 ? 'medium' : 'low')} />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Competitive Pressure by Keyword Cluster */}
      {pressure.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Competitive Pressure
          </h3>
          <div className="space-y-2">
            {pressure.slice(0, 8).map((p: any, idx: number) => (
              <div key={idx} className="flex items-center justify-between p-2 bg-slate-800/30 rounded">
                <span className="text-white text-sm truncate mr-4">{p.cluster || p.keyword || p.query}</span>
                <ThreatBadge level={p.pressure_level || p.threat_level || 'medium'} />
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Emerging Threats */}
      {emerging.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Emerging Threats ({emerging.length})
          </h3>
          <div className="space-y-2">
            {emerging.slice(0, 6).map((et: any, idx: number) => (
              <div key={idx} className="p-2 bg-red-900/20 border border-red-700/30 rounded text-sm">
                <div className="text-white font-medium">{et.domain || et.competitor}</div>
                <div className="text-xs text-slate-400 mt-1">{et.reason || et.description || `${et.new_keywords || 0} new keyword appearances`}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recommendations */}
      {data.recommendations?.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-3">Recommendations</h3>
          <ul className="space-y-2">
            {data.recommendations.slice(0, 5).map((rec: any, idx: number) => (
              <li key={idx} className="text-sm text-slate-300 flex items-start gap-2">
                <span className="text-blue-400 mt-0.5">•</span>
                <span>{typeof rec === 'string' ? rec : rec.text || rec.recommendation || JSON.stringify(rec)}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

// Module 12: Revenue Attribution
function RevenueAttributionContent({ data }: { data: any }) {
  if (!data) return <div className="text-slate-400">No data available</div>;

  // Module 12 returns: summary, revenue_by_page, top_converting_queries,
  // revenue_at_risk, position_improvement_roi, conversion_funnel,
  // revenue_concentration, recommendations, data_quality
  const funnel = data.conversion_funnel || {};
  const revenueByPage = data.revenue_by_page || [];
  const atRisk = data.revenue_at_risk || [];
  const roiOpps = data.position_improvement_roi || [];
  const topQueries = data.top_converting_queries || [];
  const concentration = data.revenue_concentration || {};
  const quality = data.data_quality || {};

  const totalRevenue = funnel.total_revenue || revenueByPage.reduce((sum: number, p: any) => sum + (p.estimated_revenue || p.revenue || 0), 0);
  const topPage = revenueByPage.length > 0 ? (revenueByPage[0].page || revenueByPage[0].url || 'N/A') : 'N/A';

  return (
    <div className="space-y-6">
      {/* Summary Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <MetricCard
          label="Estimated Revenue"
          value={`$${Math.round(totalRevenue).toLocaleString()}`}
          className="text-emerald-400"
        />
        <MetricCard
          label="Pages Analyzed"
          value={quality.pages_analyzed || revenueByPage.length || 0}
        />
        <MetricCard
          label="At-Risk Revenue"
          value={`$${Math.round(atRisk.reduce((s: number, r: any) => s + (r.revenue_at_risk || r.estimated_revenue || 0), 0)).toLocaleString()}`}
          className="text-red-400"
        />
      </div>

      {/* Revenue by Page (Top pages bar chart) */}
      {revenueByPage.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Top Revenue Pages
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={revenueByPage.slice(0, 15).map((p: any) => ({
              page: (p.page || p.url || '').replace(/^https?:\/\/[^/]+/, '').substring(0, 40),
              revenue: Math.round(p.estimated_revenue || p.revenue || 0),
            }))}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="page" tick={{ fontSize: 10, angle: -30 }} height={60} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip formatter={(value: any) => [`$${value.toLocaleString()}`, 'Revenue']} />
              <Bar dataKey="revenue" fill="#10b981" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Position Improvement ROI */}
      {roiOpps.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Position Improvement ROI
          </h3>
          <div className="space-y-2">
            {roiOpps.slice(0, 8).map((opp: any, idx: number) => (
              <div key={idx} className="flex items-center justify-between p-2 bg-slate-800/30 rounded text-sm">
                <div className="flex-1 truncate mr-4">
                  <span className="text-white">{opp.query || opp.keyword}</span>
                  {opp.current_position && (
                    <span className="text-slate-400 ml-2">pos #{opp.current_position.toFixed(1)}</span>
                  )}
                </div>
                <span className="text-emerald-400 whitespace-nowrap font-medium">
                  +${Math.round(opp.additional_revenue || opp.potential_revenue || 0).toLocaleString()}/mo
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Revenue at Risk */}
      {atRisk.length > 0 && (
        <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Revenue at Risk
          </h3>
          <div className="space-y-2">
            {atRisk.slice(0, 8).map((item: any, idx: number) => (
              <div key={idx} className="flex items-center justify-between p-2 bg-red-900/20 border border-red-700/30 rounded text-sm">
                <span className="text-white truncate mr-4">{(item.page || item.url || '').replace(/^https?:\/\/[^/]+/, '')}</span>
                <span className="text-red-400 whitespace-nowrap font-medium">
                  ${Math.round(item.revenue_at_risk || item.estimated_revenue || 0).toLocaleString()}/mo
                </span>
              </div>
            ))}
          </div>
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
    <div className="bg-slate-800/60 p-4 rounded-lg border border-slate-700/50">
      {icon && <div className="mb-2">{icon}</div>}
      <div className="text-sm text-slate-400 mb-1">{label}</div>
      <div className={`text-2xl font-bold ${className}`}>{value}</div>
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    growing: 'bg-green-100 text-green-800',
    stable: 'bg-slate-700/40 text-slate-100',
    decaying: 'bg-yellow-100 text-yellow-800',
    critical: 'bg-red-100 text-red-800',
  };

  return (
    <span
      className={`px-2 py-1 text-xs font-medium rounded ${
        colors[status] || 'bg-slate-700/40 text-slate-100'
      }`}
    >
      {status}
    </span>
  );
}

function PriorityBadge({ score }: { score: number }) {
  const color =
    score >= 80
      ? 'bg-red-100 text-red-800'
      : score >= 50
      ? 'bg-yellow-100 text-yellow-800'
      : 'bg-green-100 text-green-800';
  const label =
    score >= 80 ? 'Critical' : score >= 50 ? 'High' : 'Normal';

  return (
    <span className={`px-2 py-1 text-xs font-medium rounded ${color}`}>
      {label} ({score.toFixed(0)})
    </span>
  );
}

function ThreatBadge({ level }: { level: string }) {
  const colors: Record<string, string> = {
    high: 'bg-red-100 text-red-800',
    medium: 'bg-yellow-100 text-yellow-800',
    low: 'bg-green-100 text-green-800',
  };

  return (
    <span
      className={`px-2 py-1 text-xs font-medium rounded ${
        colors[level] || 'bg-slate-700/40 text-slate-100'
      }`}
    >
      {level}
    </span>
  );
}
