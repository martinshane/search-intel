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
interface SectionData {
  status: string;
  execution_time_seconds?: number;
  data?: any;
  error?: { type: string; message: string };
}

interface ReportRow {
  id: string;
  user_id: string;
  domain: string;
  gsc_property: string;
  ga4_property: string | null;
  status: string;
  current_module: number | null;
  progress: Record<string, string> | null;
  report_data: {
    metadata?: any;
    sections?: Record<string, SectionData>;
    errors?: any[];
    // Legacy flat format support
    [key: string]: any;
  } | null;
  created_at: string;
  completed_at: string | null;
  error_message: string | null;
}

// ---------------------------------------------------------------------------
// Helper: extract module data from report_data regardless of format
// ---------------------------------------------------------------------------
function getModuleData(report_data: ReportRow['report_data'], moduleName: string): any | null {
  if (!report_data) return null;

  // New pipeline format: report_data.sections.{name}.data
  if (report_data.sections && report_data.sections[moduleName]) {
    const section = report_data.sections[moduleName];
    if (section.status === 'success' && section.data) return section.data;
    return null;
  }

  // Legacy flat format: report_data.{name}
  if (report_data[moduleName] && typeof report_data[moduleName] === 'object') {
    return report_data[moduleName];
  }

  return null;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------
export default function ReportPage() {
  const router = useRouter();
  const { id } = router.query;
  const [report, setReport] = useState<ReportRow | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(['health_trajectory', 'gameplan']));
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // ---- Fetch report data ----
  const fetchReport = useCallback(async () => {
    if (!id) return;
    try {
      const token = typeof window !== 'undefined' ? localStorage.getItem('auth_token') || '' : '';
      const headers: Record<string, string> = { 'Content-Type': 'application/json' };
      if (token) headers['Authorization'] = `Bearer ${token}`;

      const response = await fetch(`${API_BASE}/api/v1/reports/${id}`, { headers });
      if (!response.ok) {
        if (response.status === 401) {
          router.push('/');
          return;
        }
        throw new Error(`Failed to fetch report (HTTP ${response.status})`);
      }
      const data: ReportRow = await response.json();
      setReport(data);
      setError(null);

      // Stop polling once terminal
      if (TERMINAL_STATUSES.has(data.status) && pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  }, [id, router]);

  // ---- Polling lifecycle ----
  useEffect(() => {
    if (!id) return;

    // Initial fetch
    fetchReport();

    // Start polling
    pollRef.current = setInterval(fetchReport, POLL_INTERVAL_MS);

    return () => {
      if (pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    };
  }, [id, fetchReport]);

  // ---- Helpers ----
  const toggleSection = (section: string) => {
    setExpandedSections((prev) => {
      const next = new Set(prev);
      next.has(section) ? next.delete(section) : next.add(section);
      return next;
    });
  };

  const fmt = (num: number, d = 0) =>
    new Intl.NumberFormat('en-US', { minimumFractionDigits: d, maximumFractionDigits: d }).format(num);

  const fmtCurrency = (num: number) =>
    new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 0, maximumFractionDigits: 0 }).format(num);

  const fmtDate = (s: string) =>
    new Date(s).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });

  const getTrendIcon = (direction: string) => {
    if (['strong_growth', 'growth', 'growing'].includes(direction))
      return <TrendingUp className="w-5 h-5 text-green-500" />;
    if (['strong_decline', 'decline', 'declining'].includes(direction))
      return <TrendingDown className="w-5 h-5 text-red-500" />;
    return <Minus className="w-5 h-5 text-gray-500" />;
  };

  const getBucketColor = (b: string) =>
    ({ growing: '#10b981', stable: '#6b7280', decaying: '#f59e0b', critical: '#ef4444' }[b] || '#6b7280');

  // ---- Loading state ----
  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4" />
          <p className="text-gray-600">Loading report...</p>
        </div>
      </div>
    );
  }

  // ---- Error / not found ----
  if (error || !report) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center px-4">
        <div className="text-center">
          <AlertTriangle className="w-12 h-12 text-red-500 mx-auto mb-4" />
          <h1 className="text-2xl font-bold text-gray-900 mb-2">Report Not Found</h1>
          <p className="text-gray-600 mb-6">{error || 'The requested report could not be found.'}</p>
          <button onClick={() => router.push('/')} className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition">
            Back to Home
          </button>
        </div>
      </div>
    );
  }

  // ---- In-progress state (with live polling) ----
  if (!TERMINAL_STATUSES.has(report.status)) {
    const moduleOrder = [
      'health_trajectory', 'page_triage', 'serp_landscape', 'content_intelligence',
      'gameplan', 'algorithm_impact', 'intent_migration', 'technical_health',
      'site_architecture', 'branded_split', 'competitive_threats', 'revenue_attribution',
    ];
    const progress = report.progress || {};
    const currentMod = report.current_module || 0;

    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center px-4">
        <div className="text-center max-w-lg w-full">
          <div className="relative mx-auto mb-6 w-16 h-16">
            <RefreshCw className="w-16 h-16 text-blue-600 animate-spin" style={{ animationDuration: '3s' }} />
          </div>
          <h1 className="text-2xl font-bold text-gray-900 mb-2">
            Generating Report for {report.domain || report.gsc_property}
          </h1>
          <p className="text-gray-600 mb-1">Status: <span className="font-medium capitalize">{report.status}</span></p>
          <p className="text-sm text-gray-500 mb-6">Polling every {POLL_INTERVAL_MS / 1000}s — this page updates automatically.</p>

          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 text-left">
            <h2 className="text-sm font-semibold text-gray-700 mb-4 uppercase tracking-wider">Module Progress</h2>
            <div className="space-y-3">
              {moduleOrder.map((name, idx) => {
                const num = idx + 1;
                const status = progress[`module_${num}`] || (num < currentMod ? 'success' : num === currentMod ? 'running' : 'pending');
                const meta = MODULE_META[name] || { title: name, number: num };
                return (
                  <div key={name} className="flex items-center justify-between">
                    <div className="flex items-center space-x-2 min-w-0">
                      <span className="text-xs text-gray-400 w-5 text-right flex-shrink-0">{num}</span>
                      <span className="text-sm text-gray-700 truncate">{meta.title}</span>
                    </div>
                    <span className={`px-2 py-0.5 rounded text-xs font-medium flex-shrink-0 ${
                      status === 'success' ? 'bg-green-100 text-green-800' :
                      status === 'running' ? 'bg-blue-100 text-blue-800 animate-pulse' :
                      status === 'failed' ? 'bg-red-100 text-red-800' :
                      status === 'skipped' ? 'bg-yellow-100 text-yellow-800' :
                      'bg-gray-100 text-gray-500'
                    }`}>
                      {status}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>

          <button onClick={() => router.push('/')} className="mt-6 text-blue-600 hover:text-blue-700 transition text-sm">
            Back to Home
          </button>
        </div>
      </div>
    );
  }

  // ---- Failed state ----
  if (report.status === 'failed') {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center px-4">
        <div className="text-center max-w-md">
          <AlertTriangle className="w-12 h-12 text-red-500 mx-auto mb-4" />
          <h1 className="text-2xl font-bold text-gray-900 mb-2">Report Generation Failed</h1>
          <p className="text-gray-600 mb-4">{report.error_message || 'An unexpected error occurred during report generation.'}</p>
          <div className="flex items-center justify-center space-x-4">
            <button onClick={() => router.push('/')} className="px-5 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition">
              Back to Home
            </button>
          </div>
        </div>
      </div>
    );
  }

  // ---- Completed / Partial — render full report ----
  const rd = report.report_data;
  const health = getModuleData(rd, 'health_trajectory');
  const triage = getModuleData(rd, 'page_triage');
  const serp = getModuleData(rd, 'serp_landscape');
  const content = getModuleData(rd, 'content_intelligence');
  const gameplan = getModuleData(rd, 'gameplan');
  const algo = getModuleData(rd, 'algorithm_impact');
  const intent = getModuleData(rd, 'intent_migration');
  const ctr = getModuleData(rd, 'technical_health');
  const arch = getModuleData(rd, 'site_architecture');
  const branded = getModuleData(rd, 'branded_split');
  const competitive = getModuleData(rd, 'competitive_threats');
  const revenue = getModuleData(rd, 'revenue_attribution');

  // Completion message
  const completionMsg = rd?.metadata?.completion_message || '';
  const isPartial = report.status === 'partial';

  return (
    <>
      <Head>
        <title>Search Intelligence Report — {report.domain || report.gsc_property}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      </Head>

      <div className="min-h-screen bg-gray-50">
        <NavHeader activePage="reports" />
        {/* Header */}
        <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="flex items-center justify-between h-16">
              <div className="flex-1 min-w-0">
                <h1 className="text-lg sm:text-xl font-bold text-gray-900 truncate">
                  {report.domain || report.gsc_property}
                </h1>
                <p className="text-xs sm:text-sm text-gray-500 mt-1">
                  Generated {fmtDate(report.created_at)}
                  {rd?.metadata?.execution_time_seconds && (
                    <span className="ml-2 text-gray-400">({Math.round(rd.metadata.execution_time_seconds)}s)</span>
                  )}
                </p>
              </div>
              <div className="flex items-center space-x-2 sm:space-x-4 ml-4">
                <button
                  onClick={() => window.open(`${API_BASE}/api/v1/reports/${report.id}/pdf`, '_blank')}
                  className="hidden sm:flex items-center space-x-2 px-3 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition"
                >
                  <Download className="w-4 h-4" />
                  <span>Export PDF</span>
                </button>
                <button
                  onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
                  className="sm:hidden p-2 text-gray-700 hover:bg-gray-100 rounded-lg transition"
                >
                  {mobileMenuOpen ? <X className="w-6 h-6" /> : <Menu className="w-6 h-6" />}
                </button>
              </div>
            </div>
          </div>
          {mobileMenuOpen && (
            <div className="sm:hidden border-t border-gray-200 bg-white">
              <div className="px-4 py-3 space-y-2">
                <button
                  onClick={() => window.open(`${API_BASE}/api/v1/reports/${report.id}/pdf`, '_blank')}
                  className="w-full flex items-center space-x-2 px-3 py-2 text-sm font-medium text-gray-700 bg-gray-50 rounded-lg hover:bg-gray-100 transition"
                >
                  <Download className="w-4 h-4" />
                  <span>Export PDF</span>
                </button>
              </div>
            </div>
          )}
        </header>

        {/* Partial report banner */}
        {isPartial && completionMsg && (
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 mt-4">
            <div className="bg-yellow-50 border border-yellow-200 rounded-lg px-4 py-3 flex items-start space-x-3">
              <AlertTriangle className="w-5 h-5 text-yellow-600 flex-shrink-0 mt-0.5" />
              <p className="text-sm text-yellow-800">{completionMsg}</p>
            </div>
          </div>
        )}

        {/* Main content */}
        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 sm:py-8">
          <div className="space-y-6">

            {/* ================================================================
                Section 1: Health & Trajectory
            ================================================================ */}
            {health && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button onClick={() => toggleSection('health_trajectory')} className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition">
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    {getTrendIcon(health.overall_direction)}
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">Health & Trajectory</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">
                        {health.overall_direction === 'declining' ? 'Traffic declining' :
                         health.overall_direction === 'growing' ? 'Traffic growing' : 'Traffic stable'}
                        {' '}at {Math.abs(health.trend_slope_pct_per_month || 0).toFixed(1)}%/month
                      </p>
                    </div>
                  </div>
                  {expandedSections.has('health_trajectory') ? <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" /> : <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />}
                </button>

                {expandedSections.has('health_trajectory') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {health.forecast && (
                      <div className="mb-6">
                        <h3 className="text-base sm:text-lg font-semibold text-gray-900 mb-4">Traffic Forecast</h3>
                        <div className="h-64 sm:h-80">
                          <ResponsiveContainer width="100%" height="100%">
                            <LineChart
                              data={[
                                { period: 'Current', clicks: health.current_clicks || 0 },
                                { period: '30d', clicks: health.forecast['30d']?.clicks || 0, ci_low: health.forecast['30d']?.ci_low || 0, ci_high: health.forecast['30d']?.ci_high || 0 },
                                { period: '60d', clicks: health.forecast['60d']?.clicks || 0, ci_low: health.forecast['60d']?.ci_low || 0, ci_high: health.forecast['60d']?.ci_high || 0 },
                                { period: '90d', clicks: health.forecast['90d']?.clicks || 0, ci_low: health.forecast['90d']?.ci_low || 0, ci_high: health.forecast['90d']?.ci_high || 0 },
                              ]}
                              margin={{ top: 5, right: 10, left: 0, bottom: 5 }}
                            >
                              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                              <XAxis dataKey="period" tick={{ fontSize: 12 }} />
                              <YAxis tick={{ fontSize: 12 }} />
                              <Tooltip contentStyle={{ fontSize: '12px' }} formatter={(v: number) => fmt(v)} />
                              <Legend wrapperStyle={{ fontSize: '12px' }} />
                              <Line type="monotone" dataKey="clicks" stroke="#3b82f6" strokeWidth={2} dot={{ r: 4 }} />
                              <Line type="monotone" dataKey="ci_high" stroke="#93c5fd" strokeDasharray="5 5" dot={false} name="Upper CI" />
                              <Line type="monotone" dataKey="ci_low" stroke="#93c5fd" strokeDasharray="5 5" dot={false} name="Lower CI" />
                            </LineChart>
                          </ResponsiveContainer>
                        </div>
                      </div>
                    )}

                    {health.seasonality && (
                      <div className="bg-blue-50 rounded-lg p-4 mb-6">
                        <h4 className="text-sm font-semibold text-gray-900 mb-2">Seasonality Patterns</h4>
                        <div className="text-sm text-gray-700 space-y-1">
                          <p>Best day: <span className="font-medium">{health.seasonality.best_day}</span></p>
                          <p>Worst day: <span className="font-medium">{health.seasonality.worst_day}</span></p>
                          {health.seasonality.cycle_description && <p className="mt-2">{health.seasonality.cycle_description}</p>}
                        </div>
                      </div>
                    )}

                    {health.change_points && health.change_points.length > 0 && (
                      <div>
                        <h4 className="text-sm font-semibold text-gray-900 mb-3">Significant Changes</h4>
                        <div className="overflow-x-auto -mx-4 sm:mx-0">
                          <table className="min-w-full divide-y divide-gray-200">
                            <thead className="bg-gray-50">
                              <tr>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Direction</th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Magnitude</th>
                              </tr>
                            </thead>
                            <tbody className="bg-white divide-y divide-gray-200">
                              {health.change_points.map((cp: any, idx: number) => (
                                <tr key={idx}>
                                  <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">{fmtDate(cp.date)}</td>
                                  <td className="px-4 py-3 whitespace-nowrap text-sm">
                                    <span className={`px-2 py-1 rounded-full text-xs font-medium ${cp.direction === 'drop' ? 'bg-red-100 text-red-800' : 'bg-green-100 text-green-800'}`}>
                                      {cp.direction}
                                    </span>
                                  </td>
                                  <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">{(Math.abs(cp.magnitude) * 100).toFixed(1)}%</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {/* ================================================================
                Section 2: Page-Level Triage
            ================================================================ */}
            {triage && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button onClick={() => toggleSection('page_triage')} className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition">
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    <Target className="w-5 h-5 text-orange-500 flex-shrink-0" />
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">Page-Level Triage</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">
                        {triage.summary?.critical || 0} critical pages, {fmt(triage.summary?.total_recoverable_clicks_monthly || 0)} clicks recoverable
                      </p>
                    </div>
                  </div>
                  {expandedSections.has('page_triage') ? <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" /> : <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />}
                </button>

                {expandedSections.has('page_triage') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {triage.summary && (
                      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 sm:gap-4 mb-6">
                        {[
                          { label: 'Growing',  value: triage.summary.growing  || 0, bg: 'bg-green-50',  text: 'text-green-700'  },
                          { label: 'Stable',   value: triage.summary.stable   || 0, bg: 'bg-gray-50',   text: 'text-gray-700'   },
                          { label: 'Decaying', value: triage.summary.decaying || 0, bg: 'bg-orange-50', text: 'text-orange-700' },
                          { label: 'Critical', value: triage.summary.critical || 0, bg: 'bg-red-50',    text: 'text-red-700'    },
                        ].map(({ label, value, bg, text }) => (
                          <div key={label} className={`${bg} rounded-lg p-3 sm:p-4`}>
                            <div className={`text-2xl sm:text-3xl font-bold ${text}`}>{value}</div>
                            <div className="text-xs sm:text-sm text-gray-600 mt-1">{label}</div>
                          </div>
                        ))}
                      </div>
                    )}

                    {triage.pages && triage.pages.length > 0 && (
                      <>
                        <h3 className="text-base font-semibold text-gray-900 mb-3">Top Priority Pages</h3>
                        <div className="overflow-x-auto -mx-4 sm:mx-0">
                          <table className="min-w-full divide-y divide-gray-200 text-sm">
                            <thead className="bg-gray-50">
                              <tr>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Page</th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Monthly Clicks</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Priority</th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Action</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-200">
                              {triage.pages.slice(0, 20).map((p: any, i: number) => (
                                <tr key={i} className="hover:bg-gray-50">
                                  <td className="px-4 py-3 max-w-xs truncate text-gray-900">{p.url}</td>
                                  <td className="px-4 py-3 whitespace-nowrap">
                                    <span className="px-2 py-1 rounded-full text-xs font-medium" style={{ backgroundColor: getBucketColor(p.bucket) + '20', color: getBucketColor(p.bucket) }}>
                                      {p.bucket}
                                    </span>
                                  </td>
                                  <td className="px-4 py-3 text-right text-gray-700">{fmt(p.current_monthly_clicks || 0)}</td>
                                  <td className="px-4 py-3 text-right font-medium text-gray-900">{(p.priority_score || 0).toFixed(1)}</td>
                                  <td className="px-4 py-3 whitespace-nowrap text-gray-600">{(p.recommended_action || '').replace(/_/g, ' ')}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </>
                    )}
                  </div>
                )}
              </section>
            )}

            {/* ================================================================
                Section 3: SERP Landscape
            ================================================================ */}
            {serp && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button onClick={() => toggleSection('serp_landscape')} className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition">
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    <Search className="w-5 h-5 text-purple-500 flex-shrink-0" />
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">SERP Landscape</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">
                        {serp.keywords_analyzed || 0} keywords analyzed — {((serp.total_click_share || 0) * 100).toFixed(1)}% click share
                      </p>
                    </div>
                  </div>
                  {expandedSections.has('serp_landscape') ? <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" /> : <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />}
                </button>

                {expandedSections.has('serp_landscape') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {/* Competitors */}
                    {serp.competitors && serp.competitors.length > 0 && (
                      <div className="mb-6">
                        <h3 className="text-base font-semibold text-gray-900 mb-3">Top Competitors</h3>
                        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                          {serp.competitors.slice(0, 6).map((c: any, i: number) => (
                            <div key={i} className="border border-gray-200 rounded-lg p-3">
                              <div className="font-medium text-gray-900 truncate">{c.domain}</div>
                              <div className="text-sm text-gray-600 mt-1">{c.keywords_shared} shared keywords</div>
                              <div className="flex items-center justify-between mt-2">
                                <span className="text-xs text-gray-500">Avg pos: {(c.avg_position || 0).toFixed(1)}</span>
                                <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                                  c.threat_level === 'high' ? 'bg-red-100 text-red-800' :
                                  c.threat_level === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                                  'bg-green-100 text-green-800'
                                }`}>{c.threat_level}</span>
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* SERP Feature Displacement */}
                    {serp.serp_feature_displacement && serp.serp_feature_displacement.length > 0 && (
                      <div>
                        <h3 className="text-base font-semibold text-gray-900 mb-3">SERP Feature Displacement</h3>
                        <div className="overflow-x-auto -mx-4 sm:mx-0">
                          <table className="min-w-full divide-y divide-gray-200 text-sm">
                            <thead className="bg-gray-50">
                              <tr>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Keyword</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Organic Pos</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Visual Pos</th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Features Above</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-200">
                              {serp.serp_feature_displacement.slice(0, 15).map((d: any, i: number) => (
                                <tr key={i} className="hover:bg-gray-50">
                                  <td className="px-4 py-3 text-gray-900 max-w-xs truncate">{d.keyword}</td>
                                  <td className="px-4 py-3 text-right text-gray-700">{d.organic_position}</td>
                                  <td className="px-4 py-3 text-right font-medium text-red-600">{d.visual_position}</td>
                                  <td className="px-4 py-3 text-gray-600 text-xs">{(d.features_above || []).join(', ')}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {/* ================================================================
                Section 4: Content Intelligence
            ================================================================ */}
            {content && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button onClick={() => toggleSection('content_intelligence')} className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition">
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    <FileText className="w-5 h-5 text-indigo-500 flex-shrink-0" />
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">Content Intelligence</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">
                        {content.cannibalization_clusters?.length || 0} cannibalization clusters, {content.striking_distance?.length || 0} striking distance opportunities
                      </p>
                    </div>
                  </div>
                  {expandedSections.has('content_intelligence') ? <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" /> : <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />}
                </button>

                {expandedSections.has('content_intelligence') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {/* Striking distance */}
                    {content.striking_distance && content.striking_distance.length > 0 && (
                      <div className="mb-6">
                        <h3 className="text-base font-semibold text-gray-900 mb-3">Striking Distance Opportunities</h3>
                        <div className="overflow-x-auto -mx-4 sm:mx-0">
                          <table className="min-w-full divide-y divide-gray-200 text-sm">
                            <thead className="bg-gray-50">
                              <tr>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Query</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Position</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Impressions</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Est. Click Gain</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-200">
                              {content.striking_distance.slice(0, 15).map((s: any, i: number) => (
                                <tr key={i} className="hover:bg-gray-50">
                                  <td className="px-4 py-3 text-gray-900 max-w-xs truncate">{s.query}</td>
                                  <td className="px-4 py-3 text-right text-gray-700">{(s.current_position || 0).toFixed(1)}</td>
                                  <td className="px-4 py-3 text-right text-gray-700">{fmt(s.impressions || 0)}</td>
                                  <td className="px-4 py-3 text-right font-medium text-green-700">+{fmt(s.estimated_click_gain_if_top5 || 0)}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}

                    {/* Cannibalization */}
                    {content.cannibalization_clusters && content.cannibalization_clusters.length > 0 && (
                      <div>
                        <h3 className="text-base font-semibold text-gray-900 mb-3">Cannibalization Clusters</h3>
                        <div className="space-y-3">
                          {content.cannibalization_clusters.slice(0, 10).map((c: any, i: number) => (
                            <div key={i} className="border border-gray-200 rounded-lg p-4">
                              <div className="font-medium text-gray-900 mb-1">{c.query_group}</div>
                              <div className="text-sm text-gray-600 mb-2">{c.shared_queries} shared queries — {fmt(c.total_impressions_affected)} impressions affected</div>
                              <div className="text-xs text-gray-500">
                                Pages: {(c.pages || []).join(' vs ')}
                              </div>
                              <div className="mt-2">
                                <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                                  c.recommendation === 'consolidate' ? 'bg-red-100 text-red-800' : 'bg-blue-100 text-blue-800'
                                }`}>{c.recommendation}</span>
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {/* ================================================================
                Section 5: The Gameplan
            ================================================================ */}
            {gameplan && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button onClick={() => toggleSection('gameplan')} className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition">
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    <Zap className="w-5 h-5 text-yellow-500 flex-shrink-0" />
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">The Gameplan</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">Prioritized action plan based on all analysis modules</p>
                    </div>
                  </div>
                  {expandedSections.has('gameplan') ? <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" /> : <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />}
                </button>

                {expandedSections.has('gameplan') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {/* Render each priority tier */}
                    {['critical', 'quick_wins', 'strategic', 'structural'].map((tier) => {
                      const items = gameplan[tier] || gameplan.actions?.[tier] || [];
                      if (!items.length) return null;
                      const tierLabel = tier.replace(/_/g, ' ').replace(/\b\w/g, (c: string) => c.toUpperCase());
                      const tierColor = tier === 'critical' ? 'red' : tier === 'quick_wins' ? 'green' : tier === 'strategic' ? 'blue' : 'gray';
                      return (
                        <div key={tier} className="mb-6 last:mb-0">
                          <h3 className={`text-base font-semibold text-${tierColor}-700 mb-3`}>{tierLabel}</h3>
                          <div className="space-y-2">
                            {items.map((item: any, i: number) => (
                              <div key={i} className={`border-l-4 border-${tierColor}-400 bg-${tierColor}-50 rounded-r-lg p-3`}>
                                <div className="font-medium text-gray-900 text-sm">{item.title || item.action || item.description || JSON.stringify(item)}</div>
                                {item.impact && <div className="text-xs text-gray-600 mt-1">Impact: {item.impact}</div>}
                                {item.effort && <div className="text-xs text-gray-600">Effort: {item.effort}</div>}
                              </div>
                            ))}
                          </div>
                        </div>
                      );
                    })}

                    {/* Narrative */}
                    {gameplan.narrative && (
                      <div className="mt-6 bg-gray-50 rounded-lg p-4">
                        <h4 className="text-sm font-semibold text-gray-900 mb-2">Analysis Summary</h4>
                        <p className="text-sm text-gray-700 whitespace-pre-wrap">{gameplan.narrative}</p>
                      </div>
                    )}

                    {/* Consulting CTA */}
                    <div className="mt-6 bg-blue-50 border border-blue-200 rounded-lg p-4 sm:p-6 text-center">
                      <h4 className="text-lg font-bold text-blue-900 mb-2">Want help executing this plan?</h4>
                      <p className="text-sm text-blue-700 mb-4">Our search consultants can implement these recommendations and accelerate your organic growth.</p>
                      <a href="https://clankermarketing.com/contact" target="_blank" rel="noopener noreferrer" className="inline-flex items-center space-x-2 px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition font-medium">
                        <span>Book a Free Strategy Call</span>
                        <ExternalLink className="w-4 h-4" />
                      </a>
                    </div>
                  </div>
                )}
              </section>
            )}

            {/* ================================================================
                Section 6: Algorithm Impact
            ================================================================ */}
            {algo && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button onClick={() => toggleSection('algorithm_impact')} className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition">
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    <Shield className="w-5 h-5 text-amber-500 flex-shrink-0" />
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">Algorithm Impact</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">
                        Vulnerability score: {((algo.vulnerability_score || 0) * 100).toFixed(0)}% — {algo.updates_impacting_site?.length || 0} updates detected
                      </p>
                    </div>
                  </div>
                  {expandedSections.has('algorithm_impact') ? <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" /> : <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />}
                </button>

                {expandedSections.has('algorithm_impact') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {/* Vulnerability gauge */}
                    <div className="flex items-center space-x-4 mb-6">
                      <div className="flex-shrink-0 w-20 h-20 relative">
                        <svg viewBox="0 0 36 36" className="w-20 h-20 transform -rotate-90">
                          <path d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831" fill="none" stroke="#e5e7eb" strokeWidth="3" />
                          <path d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831" fill="none"
                            stroke={algo.vulnerability_score > 0.7 ? '#ef4444' : algo.vulnerability_score > 0.4 ? '#f59e0b' : '#10b981'}
                            strokeWidth="3" strokeDasharray={`${(algo.vulnerability_score || 0) * 100}, 100`} />
                        </svg>
                        <div className="absolute inset-0 flex items-center justify-center">
                          <span className="text-sm font-bold text-gray-900">{((algo.vulnerability_score || 0) * 100).toFixed(0)}%</span>
                        </div>
                      </div>
                      <div>
                        <div className="text-sm font-semibold text-gray-900">Algorithm Vulnerability</div>
                        <div className="text-xs text-gray-600 mt-1">
                          {algo.vulnerability_score > 0.7 ? 'High risk — site is frequently impacted by algorithm updates' :
                           algo.vulnerability_score > 0.4 ? 'Moderate risk — some exposure to algorithm volatility' :
                           'Low risk — site appears algorithmically resilient'}
                        </div>
                      </div>
                    </div>

                    {algo.recommendation && (
                      <div className="bg-amber-50 border border-amber-200 rounded-lg p-4 mb-6">
                        <h4 className="text-sm font-semibold text-amber-900 mb-1">Recommendation</h4>
                        <p className="text-sm text-amber-800">{algo.recommendation}</p>
                      </div>
                    )}

                    {/* Algorithm update timeline */}
                    {algo.updates_impacting_site && algo.updates_impacting_site.length > 0 && (
                      <div>
                        <h3 className="text-base font-semibold text-gray-900 mb-3">Algorithm Updates Affecting Your Site</h3>
                        <div className="space-y-3">
                          {algo.updates_impacting_site.map((u: any, i: number) => (
                            <div key={i} className="border border-gray-200 rounded-lg p-4">
                              <div className="flex items-center justify-between mb-2">
                                <div className="font-medium text-gray-900">{u.update_name}</div>
                                <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                                  u.site_impact === 'negative' ? 'bg-red-100 text-red-800' :
                                  u.site_impact === 'positive' ? 'bg-green-100 text-green-800' :
                                  'bg-gray-100 text-gray-800'
                                }`}>{u.site_impact}</span>
                              </div>
                              <div className="flex items-center space-x-4 text-sm text-gray-600">
                                <span><Calendar className="inline w-3.5 h-3.5 mr-1" />{u.date}</span>
                                <span className={u.click_change_pct < 0 ? 'text-red-600 font-medium' : 'text-green-600 font-medium'}>
                                  {u.click_change_pct > 0 ? '+' : ''}{(u.click_change_pct || 0).toFixed(1)}% clicks
                                </span>
                                <span className={`px-2 py-0.5 rounded text-xs ${
                                  u.recovery_status === 'recovered' ? 'bg-green-100 text-green-800' :
                                  u.recovery_status === 'partial_recovery' ? 'bg-yellow-100 text-yellow-800' :
                                  'bg-red-100 text-red-800'
                                }`}>{(u.recovery_status || 'unknown').replace(/_/g, ' ')}</span>
                              </div>
                              {u.pages_most_affected && u.pages_most_affected.length > 0 && (
                                <div className="mt-2 text-xs text-gray-500">
                                  Most affected: {u.pages_most_affected.slice(0, 3).join(', ')}
                                </div>
                              )}
                              {u.common_characteristics && u.common_characteristics.length > 0 && (
                                <div className="mt-1 flex flex-wrap gap-1">
                                  {u.common_characteristics.map((c: string, ci: number) => (
                                    <span key={ci} className="px-2 py-0.5 bg-gray-100 text-gray-600 rounded text-xs">{c.replace(/_/g, ' ')}</span>
                                  ))}
                                </div>
                              )}
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {/* ================================================================
                Section 7: Intent Migration
            ================================================================ */}
            {intent && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button onClick={() => toggleSection('intent_migration')} className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition">
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    <Layers className="w-5 h-5 text-cyan-500 flex-shrink-0" />
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">Intent Migration</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">
                        {intent.ai_overview_impact?.queries_affected || 0} queries impacted by AI Overviews — est. {fmt(intent.ai_overview_impact?.estimated_monthly_clicks_lost || 0)} clicks/mo lost
                      </p>
                    </div>
                  </div>
                  {expandedSections.has('intent_migration') ? <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" /> : <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />}
                </button>

                {expandedSections.has('intent_migration') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {/* Intent distribution comparison — stacked area style via bar chart */}
                    {(intent.intent_distribution_current || intent.intent_distribution_6mo_ago) && (
                      <div className="mb-6">
                        <h3 className="text-base font-semibold text-gray-900 mb-4">Intent Distribution Shift</h3>
                        <div className="h-64">
                          <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={[
                              {
                                period: '6 Months Ago',
                                Informational: ((intent.intent_distribution_6mo_ago?.informational || 0) * 100),
                                Commercial: ((intent.intent_distribution_6mo_ago?.commercial || 0) * 100),
                                Navigational: ((intent.intent_distribution_6mo_ago?.navigational || 0) * 100),
                                Transactional: ((intent.intent_distribution_6mo_ago?.transactional || 0) * 100),
                              },
                              {
                                period: 'Current',
                                Informational: ((intent.intent_distribution_current?.informational || 0) * 100),
                                Commercial: ((intent.intent_distribution_current?.commercial || 0) * 100),
                                Navigational: ((intent.intent_distribution_current?.navigational || 0) * 100),
                                Transactional: ((intent.intent_distribution_current?.transactional || 0) * 100),
                              },
                            ]} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                              <XAxis dataKey="period" tick={{ fontSize: 12 }} />
                              <YAxis tick={{ fontSize: 12 }} unit="%" />
                              <Tooltip contentStyle={{ fontSize: '12px' }} formatter={(v: number) => `${v.toFixed(1)}%`} />
                              <Legend wrapperStyle={{ fontSize: '12px' }} />
                              <Bar dataKey="Informational" stackId="a" fill="#3b82f6" />
                              <Bar dataKey="Commercial" stackId="a" fill="#8b5cf6" />
                              <Bar dataKey="Navigational" stackId="a" fill="#06b6d4" />
                              <Bar dataKey="Transactional" stackId="a" fill="#10b981" />
                            </BarChart>
                          </ResponsiveContainer>
                        </div>
                      </div>
                    )}

                    {/* AI Overview impact callout */}
                    {intent.ai_overview_impact && intent.ai_overview_impact.queries_affected > 0 && (
                      <div className="bg-cyan-50 border border-cyan-200 rounded-lg p-4 mb-6">
                        <h4 className="text-sm font-semibold text-cyan-900 mb-2">AI Overview Displacement</h4>
                        <div className="grid grid-cols-2 gap-4">
                          <div>
                            <div className="text-2xl font-bold text-cyan-700">{intent.ai_overview_impact.queries_affected}</div>
                            <div className="text-xs text-cyan-600">Queries affected</div>
                          </div>
                          <div>
                            <div className="text-2xl font-bold text-red-600">-{fmt(intent.ai_overview_impact.estimated_monthly_clicks_lost)}</div>
                            <div className="text-xs text-cyan-600">Clicks/month lost</div>
                          </div>
                        </div>
                        {intent.ai_overview_impact.affected_queries && intent.ai_overview_impact.affected_queries.length > 0 && (
                          <div className="mt-3 text-xs text-cyan-700">
                            Top affected: {intent.ai_overview_impact.affected_queries.slice(0, 5).map((q: any) => typeof q === 'string' ? q : q.query || q.keyword).join(', ')}
                          </div>
                        )}
                      </div>
                    )}

                    {/* Strategic recommendation */}
                    {intent.strategic_recommendation && (
                      <div className="bg-gray-50 rounded-lg p-4">
                        <h4 className="text-sm font-semibold text-gray-900 mb-1">Strategic Recommendation</h4>
                        <p className="text-sm text-gray-700">{intent.strategic_recommendation}</p>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {/* ================================================================
                Section 8: CTR Modeling
            ================================================================ */}
            {ctr && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button onClick={() => toggleSection('technical_health')} className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition">
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    <BarChart2 className="w-5 h-5 text-teal-500 flex-shrink-0" />
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">CTR Modeling</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">
                        Model R²: {((ctr.ctr_model_accuracy || 0) * 100).toFixed(0)}% — {ctr.keyword_ctr_analysis?.length || 0} keywords analyzed
                      </p>
                    </div>
                  </div>
                  {expandedSections.has('technical_health') ? <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" /> : <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />}
                </button>

                {expandedSections.has('technical_health') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {/* Expected vs Actual CTR scatter plot */}
                    {ctr.keyword_ctr_analysis && ctr.keyword_ctr_analysis.length > 0 && (
                      <div className="mb-6">
                        <h3 className="text-base font-semibold text-gray-900 mb-4">Expected vs Actual CTR</h3>
                        <div className="h-72">
                          <ResponsiveContainer width="100%" height="100%">
                            <ScatterChart margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                              <XAxis type="number" dataKey="expected" name="Expected CTR" tick={{ fontSize: 11 }}
                                tickFormatter={(v: number) => `${(v * 100).toFixed(0)}%`} label={{ value: 'Expected CTR (contextual)', position: 'insideBottom', offset: -2, style: { fontSize: 11 } }} />
                              <YAxis type="number" dataKey="actual" name="Actual CTR" tick={{ fontSize: 11 }}
                                tickFormatter={(v: number) => `${(v * 100).toFixed(0)}%`} label={{ value: 'Actual CTR', angle: -90, position: 'insideLeft', style: { fontSize: 11 } }} />
                              <Tooltip contentStyle={{ fontSize: '12px' }} formatter={(v: number) => `${(v * 100).toFixed(2)}%`}
                                labelFormatter={() => ''} />
                              <Scatter name="Keywords" data={ctr.keyword_ctr_analysis.slice(0, 100).map((k: any) => ({
                                expected: k.expected_ctr_contextual || k.expected_ctr_generic || 0,
                                actual: k.actual_ctr || 0,
                                keyword: k.keyword,
                              }))} fill="#14b8a6">
                                {ctr.keyword_ctr_analysis.slice(0, 100).map((_: any, i: number) => (
                                  <Cell key={i} fill={
                                    (ctr.keyword_ctr_analysis[i].actual_ctr || 0) > (ctr.keyword_ctr_analysis[i].expected_ctr_contextual || ctr.keyword_ctr_analysis[i].expected_ctr_generic || 0)
                                      ? '#10b981' : '#ef4444'
                                  } />
                                ))}
                              </Scatter>
                              <ReferenceLine segment={[{ x: 0, y: 0 }, { x: 0.3, y: 0.3 }]} stroke="#9ca3af" strokeDasharray="5 5" />
                            </ScatterChart>
                          </ResponsiveContainer>
                        </div>
                        <p className="text-xs text-gray-500 mt-2 text-center">Green = overperforming vs SERP context, Red = underperforming. Diagonal = expected.</p>
                      </div>
                    )}

                    {/* Feature opportunities */}
                    {ctr.feature_opportunities && ctr.feature_opportunities.length > 0 && (
                      <div>
                        <h3 className="text-base font-semibold text-gray-900 mb-3">SERP Feature Opportunities</h3>
                        <div className="overflow-x-auto -mx-4 sm:mx-0">
                          <table className="min-w-full divide-y divide-gray-200 text-sm">
                            <thead className="bg-gray-50">
                              <tr>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Keyword</th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Feature</th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Current Holder</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Est. Click Gain</th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Difficulty</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-200">
                              {ctr.feature_opportunities.slice(0, 15).map((o: any, i: number) => (
                                <tr key={i} className="hover:bg-gray-50">
                                  <td className="px-4 py-3 text-gray-900 max-w-xs truncate">{o.keyword}</td>
                                  <td className="px-4 py-3 whitespace-nowrap">
                                    <span className="px-2 py-0.5 rounded text-xs font-medium bg-teal-100 text-teal-800">{(o.feature || '').replace(/_/g, ' ')}</span>
                                  </td>
                                  <td className="px-4 py-3 text-gray-600 max-w-xs truncate">{o.current_holder || '—'}</td>
                                  <td className="px-4 py-3 text-right font-medium text-green-700">+{fmt(o.estimated_click_gain || 0)}</td>
                                  <td className="px-4 py-3 whitespace-nowrap">
                                    <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                                      o.difficulty === 'easy' ? 'bg-green-100 text-green-800' :
                                      o.difficulty === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                                      'bg-red-100 text-red-800'
                                    }`}>{o.difficulty}</span>
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {/* ================================================================
                Section 9: Site Architecture
            ================================================================ */}
            {arch && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button onClick={() => toggleSection('site_architecture')} className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition">
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    <Globe className="w-5 h-5 text-violet-500 flex-shrink-0" />
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">Site Architecture</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">
                        {((arch.authority_flow_to_conversion || 0) * 100).toFixed(1)}% authority reaching conversion pages — {arch.orphan_pages?.length || 0} orphan pages
                      </p>
                    </div>
                  </div>
                  {expandedSections.has('site_architecture') ? <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" /> : <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />}
                </button>

                {expandedSections.has('site_architecture') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {/* D3 Force-directed network graph */}
                    {(arch.graph_nodes || arch.link_graph_nodes) && (
                      <div className="mb-6">
                        <NetworkGraph
                          nodes={(arch.graph_nodes || arch.link_graph_nodes || []).map((n: any) => ({
                            id: n.id || n.url || n.page || '',
                            pagerank: n.pagerank || n.page_rank || 0,
                            silo: n.silo || n.cluster || n.category || 'unknown',
                            internal_links_in: n.internal_links_in || n.inbound_links || 0,
                            internal_links_out: n.internal_links_out || n.outbound_links || 0,
                            is_orphan: n.is_orphan || false,
                          }))}
                          links={(arch.graph_edges || arch.link_graph_edges || []).map((e: any) => ({
                            source: e.source || e.from || e.from_url || '',
                            target: e.target || e.to || e.to_url || '',
                            weight: e.weight || 1,
                          }))}
                        />
                      </div>
                    )}

                    {/* Content silos */}
                    {arch.content_silos && arch.content_silos.length > 0 && (
                      <div className="mb-6">
                        <h3 className="text-base font-semibold text-gray-900 mb-4">Content Silos</h3>
                        <div className="h-56">
                          <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={arch.content_silos.slice(0, 8).map((s: any) => ({
                              name: s.name || s.silo || 'Unknown',
                              pages: s.pages || 0,
                              authority: ((s.internal_pagerank_share || 0) * 100),
                            }))} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                              <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                              <YAxis yAxisId="left" tick={{ fontSize: 11 }} label={{ value: 'Pages', angle: -90, position: 'insideLeft', style: { fontSize: 11 } }} />
                              <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 11 }} unit="%" label={{ value: 'Authority %', angle: 90, position: 'insideRight', style: { fontSize: 11 } }} />
                              <Tooltip contentStyle={{ fontSize: '12px' }} />
                              <Legend wrapperStyle={{ fontSize: '12px' }} />
                              <Bar yAxisId="left" dataKey="pages" fill="#8b5cf6" name="Pages" />
                              <Bar yAxisId="right" dataKey="authority" fill="#c4b5fd" name="Authority %" />
                            </BarChart>
                          </ResponsiveContainer>
                        </div>
                      </div>
                    )}

                    {/* PageRank distribution */}
                    {arch.pagerank_distribution && (
                      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-6">
                        {arch.pagerank_distribution.top_authority_pages && (
                          <div className="bg-violet-50 rounded-lg p-4">
                            <h4 className="text-sm font-semibold text-violet-900 mb-2">Top Authority Pages</h4>
                            <div className="space-y-1 text-xs text-violet-700">
                              {arch.pagerank_distribution.top_authority_pages.slice(0, 5).map((p: any, i: number) => (
                                <div key={i} className="truncate">{typeof p === 'string' ? p : p.url || p.page || JSON.stringify(p)}</div>
                              ))}
                            </div>
                          </div>
                        )}
                        {arch.pagerank_distribution.starved_pages && (
                          <div className="bg-red-50 rounded-lg p-4">
                            <h4 className="text-sm font-semibold text-red-900 mb-2">Starved Pages</h4>
                            <div className="space-y-1 text-xs text-red-700">
                              {arch.pagerank_distribution.starved_pages.slice(0, 5).map((p: any, i: number) => (
                                <div key={i} className="truncate">{typeof p === 'string' ? p : p.url || p.page || JSON.stringify(p)}</div>
                              ))}
                            </div>
                          </div>
                        )}
                        {arch.pagerank_distribution.authority_sinks && (
                          <div className="bg-yellow-50 rounded-lg p-4">
                            <h4 className="text-sm font-semibold text-yellow-900 mb-2">Authority Sinks</h4>
                            <div className="space-y-1 text-xs text-yellow-700">
                              {arch.pagerank_distribution.authority_sinks.slice(0, 5).map((p: any, i: number) => (
                                <div key={i} className="truncate">{typeof p === 'string' ? p : p.url || p.page || JSON.stringify(p)}</div>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    )}

                    {/* Link recommendations */}
                    {arch.link_recommendations && arch.link_recommendations.length > 0 && (
                      <div>
                        <h3 className="text-base font-semibold text-gray-900 mb-3">Recommended Internal Links</h3>
                        <div className="overflow-x-auto -mx-4 sm:mx-0">
                          <table className="min-w-full divide-y divide-gray-200 text-sm">
                            <thead className="bg-gray-50">
                              <tr>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Link From</th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Target Page</th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Anchor Text</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">PR Boost</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-200">
                              {arch.link_recommendations.slice(0, 10).map((r: any, i: number) => (
                                <tr key={i} className="hover:bg-gray-50">
                                  <td className="px-4 py-3 text-gray-700 max-w-xs truncate">{r.link_from}</td>
                                  <td className="px-4 py-3 text-gray-900 max-w-xs truncate font-medium">{r.target_page}</td>
                                  <td className="px-4 py-3 text-violet-700 text-xs">{r.suggested_anchor}</td>
                                  <td className="px-4 py-3 text-right text-green-700 font-medium">+{((r.estimated_pagerank_boost || 0) * 100).toFixed(2)}%</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}

                    {/* Orphan pages */}
                    {arch.orphan_pages && arch.orphan_pages.length > 0 && (
                      <div className="mt-6">
                        <h3 className="text-base font-semibold text-gray-900 mb-3">Orphan Pages ({arch.orphan_pages.length})</h3>
                        <div className="bg-red-50 rounded-lg p-4">
                          <div className="space-y-1 text-sm text-red-700">
                            {arch.orphan_pages.slice(0, 10).map((p: any, i: number) => (
                              <div key={i} className="truncate">{typeof p === 'string' ? p : p.url || JSON.stringify(p)}</div>
                            ))}
                            {arch.orphan_pages.length > 10 && (
                              <div className="text-xs text-red-500 mt-2">...and {arch.orphan_pages.length - 10} more</div>
                            )}
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {/* ================================================================
                Section 10: Branded vs Non-Branded
            ================================================================ */}
            {branded && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button onClick={() => toggleSection('branded_split')} className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition">
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    <Users className="w-5 h-5 text-pink-500 flex-shrink-0" />
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">Branded vs Non-Branded</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">
                        {((branded.branded_ratio || 0) * 100).toFixed(0)}% branded — {branded.dependency_level || 'unknown'} dependency
                      </p>
                    </div>
                  </div>
                  {expandedSections.has('branded_split') ? <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" /> : <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />}
                </button>

                {expandedSections.has('branded_split') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {/* Dependency gauge + key metrics */}
                    <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 sm:gap-4 mb-6">
                      <div className={`rounded-lg p-3 sm:p-4 ${
                        branded.dependency_level === 'critical' ? 'bg-red-50' :
                        branded.dependency_level === 'high' ? 'bg-orange-50' :
                        branded.dependency_level === 'balanced' ? 'bg-yellow-50' :
                        'bg-green-50'
                      }`}>
                        <div className={`text-2xl sm:text-3xl font-bold ${
                          branded.dependency_level === 'critical' ? 'text-red-700' :
                          branded.dependency_level === 'high' ? 'text-orange-700' :
                          branded.dependency_level === 'balanced' ? 'text-yellow-700' :
                          'text-green-700'
                        }`}>{((branded.branded_ratio || 0) * 100).toFixed(0)}%</div>
                        <div className="text-xs sm:text-sm text-gray-600 mt-1">Branded Ratio</div>
                      </div>
                      <div className="bg-gray-50 rounded-lg p-3 sm:p-4">
                        <div className="text-2xl sm:text-3xl font-bold text-gray-900 capitalize">{(branded.dependency_level || 'unknown').replace(/_/g, ' ')}</div>
                        <div className="text-xs sm:text-sm text-gray-600 mt-1">Dependency Level</div>
                      </div>
                      {branded.branded_trend && (
                        <div className="bg-blue-50 rounded-lg p-3 sm:p-4">
                          <div className="flex items-center space-x-1">
                            {getTrendIcon(branded.branded_trend.direction)}
                            <span className="text-lg font-bold text-gray-900">{((branded.branded_trend.slope || 0) * 100).toFixed(1)}%</span>
                          </div>
                          <div className="text-xs sm:text-sm text-gray-600 mt-1">Branded Trend</div>
                        </div>
                      )}
                      {branded.non_branded_trend && (
                        <div className="bg-purple-50 rounded-lg p-3 sm:p-4">
                          <div className="flex items-center space-x-1">
                            {getTrendIcon(branded.non_branded_trend.direction)}
                            <span className="text-lg font-bold text-gray-900">{((branded.non_branded_trend.slope || 0) * 100).toFixed(1)}%</span>
                          </div>
                          <div className="text-xs sm:text-sm text-gray-600 mt-1">Non-Branded Trend</div>
                        </div>
                      )}
                    </div>

                    {/* Non-branded opportunity */}
                    {branded.non_branded_opportunity && (
                      <div className="bg-purple-50 border border-purple-200 rounded-lg p-4 mb-6">
                        <h4 className="text-sm font-semibold text-purple-900 mb-3">Non-Branded Opportunity</h4>
                        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                          <div>
                            <div className="text-lg font-bold text-purple-700">{fmt(branded.non_branded_opportunity.current_monthly_clicks || 0)}</div>
                            <div className="text-xs text-purple-600">Current Clicks/Mo</div>
                          </div>
                          <div>
                            <div className="text-lg font-bold text-purple-700">{fmt(branded.non_branded_opportunity.potential_monthly_clicks || 0)}</div>
                            <div className="text-xs text-purple-600">Potential Clicks/Mo</div>
                          </div>
                          <div>
                            <div className="text-lg font-bold text-green-700">+{fmt(branded.non_branded_opportunity.gap || 0)}</div>
                            <div className="text-xs text-purple-600">Click Gap</div>
                          </div>
                          <div>
                            <div className="text-lg font-bold text-purple-700">
                              {branded.non_branded_opportunity.months_to_meaningful_with_actions || '?'} mo
                            </div>
                            <div className="text-xs text-purple-600">To Meaningful (w/ action)</div>
                          </div>
                        </div>
                      </div>
                    )}

                    {/* Branded vs non-branded dual trend chart */}
                    {branded.branded_trend && branded.non_branded_trend && (
                      <div className="h-56">
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={[
                            { name: 'Branded', trend: ((branded.branded_trend.slope || 0) * 100), fill: '#ec4899' },
                            { name: 'Non-Branded', trend: ((branded.non_branded_trend.slope || 0) * 100), fill: '#a855f7' },
                          ]} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                            <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                            <YAxis tick={{ fontSize: 12 }} unit="%" label={{ value: 'Growth Rate (%/mo)', angle: -90, position: 'insideLeft', style: { fontSize: 11 } }} />
                            <Tooltip contentStyle={{ fontSize: '12px' }} formatter={(v: number) => `${v.toFixed(2)}%/mo`} />
                            <Bar dataKey="trend" name="Monthly Growth Rate">
                              {[branded.branded_trend, branded.non_branded_trend].map((t: any, i: number) => (
                                <Cell key={i} fill={i === 0 ? '#ec4899' : '#a855f7'} />
                              ))}
                            </Bar>
                            <ReferenceLine y={0} stroke="#9ca3af" strokeDasharray="3 3" />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {/* ================================================================
                Section 11: Competitive Radar
            ================================================================ */}
            {competitive && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button onClick={() => toggleSection('competitive_threats')} className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition">
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    <Users className="w-5 h-5 text-rose-500 flex-shrink-0" />
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">Competitive Radar</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">
                        {competitive.primary_competitors?.length || 0} competitors tracked — {competitive.emerging_threats?.length || 0} emerging threats
                      </p>
                    </div>
                  </div>
                  {expandedSections.has('competitive_threats') ? <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" /> : <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />}
                </button>

                {expandedSections.has('competitive_threats') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {/* Primary competitors — horizontal bar chart */}
                    {competitive.primary_competitors && competitive.primary_competitors.length > 0 && (
                      <div className="mb-6">
                        <h3 className="text-base font-semibold text-gray-900 mb-4">Primary Competitors</h3>
                        <div className="h-64">
                          <ResponsiveContainer width="100%" height="100%">
                            <BarChart layout="vertical" data={competitive.primary_competitors.slice(0, 8).map((c: any) => ({
                              domain: c.domain,
                              overlap: c.keyword_overlap || 0,
                              position: c.avg_position || 0,
                            }))} margin={{ top: 5, right: 10, left: 80, bottom: 5 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                              <XAxis type="number" tick={{ fontSize: 11 }} label={{ value: 'Keyword Overlap', position: 'insideBottom', offset: -2, style: { fontSize: 11 } }} />
                              <YAxis type="category" dataKey="domain" tick={{ fontSize: 11 }} width={75} />
                              <Tooltip contentStyle={{ fontSize: '12px' }} />
                              <Bar dataKey="overlap" fill="#f43f5e" name="Shared Keywords" />
                            </BarChart>
                          </ResponsiveContainer>
                        </div>
                      </div>
                    )}

                    {/* Emerging threats */}
                    {competitive.emerging_threats && competitive.emerging_threats.length > 0 && (
                      <div className="mb-6">
                        <h3 className="text-base font-semibold text-gray-900 mb-3">Emerging Threats</h3>
                        <div className="space-y-3">
                          {competitive.emerging_threats.map((t: any, i: number) => (
                            <div key={i} className="border border-gray-200 rounded-lg p-4">
                              <div className="flex items-center justify-between mb-2">
                                <div className="font-medium text-gray-900">{t.domain}</div>
                                <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                                  t.threat_level === 'high' ? 'bg-red-100 text-red-800' :
                                  t.threat_level === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                                  'bg-green-100 text-green-800'
                                }`}>{t.threat_level} threat</span>
                              </div>
                              <div className="flex flex-wrap items-center gap-3 text-sm text-gray-600">
                                <span>First seen: {t.first_seen}</span>
                                <span>{t.keywords_entered} keywords entered</span>
                                <span>Avg position: {(t.avg_entry_position || 0).toFixed(1)} → {(t.current_avg_position || 0).toFixed(1)}</span>
                                <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                                  t.trajectory === 'rapidly_improving' ? 'bg-red-100 text-red-800' :
                                  t.trajectory === 'improving' ? 'bg-yellow-100 text-yellow-800' :
                                  'bg-gray-100 text-gray-800'
                                }`}>{(t.trajectory || '').replace(/_/g, ' ')}</span>
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Keyword vulnerability */}
                    {competitive.keyword_vulnerability && competitive.keyword_vulnerability.length > 0 && (
                      <div>
                        <h3 className="text-base font-semibold text-gray-900 mb-3">Keyword Vulnerability</h3>
                        <div className="overflow-x-auto -mx-4 sm:mx-0">
                          <table className="min-w-full divide-y divide-gray-200 text-sm">
                            <thead className="bg-gray-50">
                              <tr>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Keyword</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Your Position</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Competitors Within 3</th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Gap Trend</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-200">
                              {competitive.keyword_vulnerability.slice(0, 15).map((k: any, i: number) => (
                                <tr key={i} className="hover:bg-gray-50">
                                  <td className="px-4 py-3 text-gray-900 max-w-xs truncate">{k.keyword}</td>
                                  <td className="px-4 py-3 text-right text-gray-700">{k.your_position}</td>
                                  <td className="px-4 py-3 text-right font-medium text-gray-900">{k.competitors_within_3}</td>
                                  <td className="px-4 py-3 whitespace-nowrap">
                                    <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                                      k.gap_trend === 'narrowing' ? 'bg-red-100 text-red-800' :
                                      k.gap_trend === 'widening' ? 'bg-green-100 text-green-800' :
                                      'bg-gray-100 text-gray-800'
                                    }`}>{k.gap_trend}</span>
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {/* ================================================================
                Section 12: Revenue Attribution
            ================================================================ */}
            {revenue && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button onClick={() => toggleSection('revenue_attribution')} className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition">
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    <DollarSign className="w-5 h-5 text-emerald-500 flex-shrink-0" />
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">Revenue Attribution</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">
                        {fmtCurrency(revenue.total_search_attributed_revenue_monthly || 0)}/mo attributed — {fmtCurrency(revenue.revenue_at_risk_90d || 0)} at risk (90d)
                      </p>
                    </div>
                  </div>
                  {expandedSections.has('revenue_attribution') ? <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" /> : <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />}
                </button>

                {expandedSections.has('revenue_attribution') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {/* Key revenue metrics */}
                    <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 sm:gap-4 mb-6">
                      <div className="bg-emerald-50 rounded-lg p-3 sm:p-4">
                        <div className="text-2xl sm:text-3xl font-bold text-emerald-700">{fmtCurrency(revenue.total_search_attributed_revenue_monthly || 0)}</div>
                        <div className="text-xs sm:text-sm text-gray-600 mt-1">Monthly Search Revenue</div>
                      </div>
                      <div className="bg-red-50 rounded-lg p-3 sm:p-4">
                        <div className="text-2xl sm:text-3xl font-bold text-red-700">{fmtCurrency(revenue.revenue_at_risk_90d || 0)}</div>
                        <div className="text-xs sm:text-sm text-gray-600 mt-1">Revenue at Risk (90d)</div>
                      </div>
                      {revenue.action_roi && (
                        <div className="bg-blue-50 rounded-lg p-3 sm:p-4">
                          <div className="text-2xl sm:text-3xl font-bold text-blue-700">{fmtCurrency(revenue.action_roi.total_opportunity || 0)}</div>
                          <div className="text-xs sm:text-sm text-gray-600 mt-1">Total Opportunity</div>
                        </div>
                      )}
                    </div>

                    {/* Action ROI waterfall-style breakdown */}
                    {revenue.action_roi && (
                      <div className="mb-6">
                        <h3 className="text-base font-semibold text-gray-900 mb-4">ROI by Action Type</h3>
                        <div className="h-56">
                          <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={[
                              { name: 'Critical Fixes', value: revenue.action_roi.critical_fixes_monthly_value || 0 },
                              { name: 'Quick Wins', value: revenue.action_roi.quick_wins_monthly_value || 0 },
                              { name: 'Strategic Plays', value: revenue.action_roi.strategic_plays_monthly_value || 0 },
                              { name: 'Total', value: revenue.action_roi.total_opportunity || 0 },
                            ]} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                              <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                              <YAxis tick={{ fontSize: 11 }} tickFormatter={(v: number) => `$${(v / 1000).toFixed(0)}k`} />
                              <Tooltip contentStyle={{ fontSize: '12px' }} formatter={(v: number) => fmtCurrency(v)} />
                              <Bar dataKey="value" name="Monthly Value">
                                <Cell fill="#f87171" />
                                <Cell fill="#34d399" />
                                <Cell fill="#60a5fa" />
                                <Cell fill="#10b981" />
                              </Bar>
                            </BarChart>
                          </ResponsiveContainer>
                        </div>
                      </div>
                    )}

                    {/* Top revenue keywords */}
                    {revenue.top_revenue_keywords && revenue.top_revenue_keywords.length > 0 && (
                      <div>
                        <h3 className="text-base font-semibold text-gray-900 mb-3">Top Revenue Keywords</h3>
                        <div className="overflow-x-auto -mx-4 sm:mx-0">
                          <table className="min-w-full divide-y divide-gray-200 text-sm">
                            <thead className="bg-gray-50">
                              <tr>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Keyword</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Current Rev/Mo</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Potential (Top 3)</th>
                                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Gap</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-200">
                              {revenue.top_revenue_keywords.slice(0, 15).map((k: any, i: number) => (
                                <tr key={i} className="hover:bg-gray-50">
                                  <td className="px-4 py-3 text-gray-900 max-w-xs truncate">{k.keyword}</td>
                                  <td className="px-4 py-3 text-right text-gray-700">{fmtCurrency(k.current_revenue_monthly || 0)}</td>
                                  <td className="px-4 py-3 text-right text-gray-700">{fmtCurrency(k.potential_revenue_if_top3 || 0)}</td>
                                  <td className="px-4 py-3 text-right font-medium text-green-700">+{fmtCurrency(k.gap || 0)}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {/* ================================================================
                Revenue CTA (bottom of report)
            ================================================================ */}
            {revenue && (
              <div className="bg-gradient-to-r from-blue-600 to-indigo-700 rounded-lg p-6 sm:p-8 text-center text-white">
                <h3 className="text-xl sm:text-2xl font-bold mb-2">Unlock Your Search Revenue Potential</h3>
                <p className="text-blue-100 mb-6 max-w-2xl mx-auto">
                  This report identified opportunities worth an estimated {fmtCurrency(revenue.total_potential_revenue || revenue.estimated_monthly_potential || 0)}/month.
                  Let us help you capture that value.
                </p>
                <a href="https://clankermarketing.com/contact" target="_blank" rel="noopener noreferrer" className="inline-flex items-center space-x-2 px-8 py-3 bg-white text-blue-700 rounded-lg hover:bg-blue-50 transition font-bold">
                  <span>Get Your Custom Growth Plan</span>
                  <ExternalLink className="w-4 h-4" />
                </a>
              </div>
            )}

          </div>
        </main>

        {/* Footer */}
        <footer className="border-t border-gray-200 bg-white mt-12">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 text-center text-sm text-gray-500">
            <p>Search Intelligence Report by <a href="https://clankermarketing.com" target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:text-blue-700">Clanker Marketing</a></p>
          </div>
        </footer>
      </div>
    </>
  );
}
