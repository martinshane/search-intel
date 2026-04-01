import { useEffect, useState, useCallback, useRef } from 'react';
import { useRouter } from 'next/router';
import Head from 'next/head';
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
                Sections 6-12: Generic collapsible sections
            ================================================================ */}
            {[
              { key: 'algorithm_impact', data: algo, icon: <Shield className="w-5 h-5 text-amber-500 flex-shrink-0" /> },
              { key: 'intent_migration', data: intent, icon: <Layers className="w-5 h-5 text-cyan-500 flex-shrink-0" /> },
              { key: 'technical_health', data: ctr, icon: <BarChart2 className="w-5 h-5 text-teal-500 flex-shrink-0" /> },
              { key: 'site_architecture', data: arch, icon: <Globe className="w-5 h-5 text-violet-500 flex-shrink-0" /> },
              { key: 'branded_split', data: branded, icon: <Users className="w-5 h-5 text-pink-500 flex-shrink-0" /> },
              { key: 'competitive_threats', data: competitive, icon: <Users className="w-5 h-5 text-rose-500 flex-shrink-0" /> },
              { key: 'revenue_attribution', data: revenue, icon: <DollarSign className="w-5 h-5 text-emerald-500 flex-shrink-0" /> },
            ].filter(({ data }) => data != null).map(({ key, data, icon }) => {
              const meta = MODULE_META[key] || { title: key, number: 0 };
              return (
                <section key={key} className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                  <button onClick={() => toggleSection(key)} className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition">
                    <div className="flex items-center space-x-3 min-w-0 flex-1">
                      {icon}
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">{meta.title}</h2>
                    </div>
                    {expandedSections.has(key) ? <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" /> : <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />}
                  </button>

                  {expandedSections.has(key) && (
                    <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                      {/* Render key stats as cards */}
                      {typeof data === 'object' && (
                        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                          {Object.entries(data).filter(([k, v]) => typeof v === 'number' || typeof v === 'string').slice(0, 9).map(([k, v]) => (
                            <div key={k} className="bg-gray-50 rounded-lg p-3">
                              <div className="text-xs text-gray-500 uppercase tracking-wider">{k.replace(/_/g, ' ')}</div>
                              <div className="text-lg font-bold text-gray-900 mt-1">
                                {typeof v === 'number' ? (Math.abs(v as number) < 1 ? `${((v as number) * 100).toFixed(1)}%` : fmt(v as number, 1)) : String(v)}
                              </div>
                            </div>
                          ))}
                        </div>
                      )}

                      {/* Render arrays as tables */}
                      {typeof data === 'object' && Object.entries(data).filter(([, v]) => Array.isArray(v) && (v as any[]).length > 0).slice(0, 2).map(([k, v]) => {
                        const arr = v as any[];
                        const cols = arr[0] ? Object.keys(arr[0]).slice(0, 5) : [];
                        if (!cols.length) return null;
                        return (
                          <div key={k} className="mt-6">
                            <h3 className="text-base font-semibold text-gray-900 mb-3">{k.replace(/_/g, ' ').replace(/\b\w/g, (c: string) => c.toUpperCase())}</h3>
                            <div className="overflow-x-auto -mx-4 sm:mx-0">
                              <table className="min-w-full divide-y divide-gray-200 text-sm">
                                <thead className="bg-gray-50">
                                  <tr>
                                    {cols.map((col) => (
                                      <th key={col} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">{col.replace(/_/g, ' ')}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody className="divide-y divide-gray-200">
                                  {arr.slice(0, 15).map((row: any, ri: number) => (
                                    <tr key={ri} className="hover:bg-gray-50">
                                      {cols.map((col) => (
                                        <td key={col} className="px-4 py-3 whitespace-nowrap text-gray-700 max-w-xs truncate">
                                          {typeof row[col] === 'number' ? fmt(row[col], 2) : Array.isArray(row[col]) ? row[col].join(', ') : String(row[col] ?? '')}
                                        </td>
                                      ))}
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </section>
              );
            })}

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
