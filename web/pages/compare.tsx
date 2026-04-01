import { useEffect, useState, useCallback } from 'react';
import { useRouter } from 'next/router';
import Head from 'next/head';
import Link from 'next/link';
import NavHeader from '../components/NavHeader';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,
} from 'recharts';
import {
  ArrowLeft,
  TrendingUp,
  TrendingDown,
  Minus,
  AlertTriangle,
  CheckCircle,
  GitCompare,
  Calendar,
  ChevronDown,
  ChevronUp,
  RefreshCw,
  Loader2,
} from 'lucide-react';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const API_BASE = process.env.NEXT_PUBLIC_API_URL || '';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
interface ReportHistoryItem {
  id: string;
  domain: string;
  status: string;
  created_at: string;
  completed_at?: string;
}

interface ModuleDelta {
  module: number;
  name: string;
  [key: string]: any;
}

interface ExecutiveSummary {
  highlights: Array<{ module: string; signal: string; detail: string }>;
  warnings: Array<{ module: string; signal: string; detail: string }>;
  total_highlights: number;
  total_warnings: number;
  overall_sentiment: string;
}

interface ComparisonResult {
  metadata: {
    current_report_id: string;
    baseline_report_id: string;
    current_domain: string;
    current_created_at: string;
    baseline_created_at: string;
    compared_at: string;
  };
  executive_summary: ExecutiveSummary;
  module_deltas: ModuleDelta[];
  modules_compared: number;
  modules_missing: number[];
}

// ---------------------------------------------------------------------------
// Utility components
// ---------------------------------------------------------------------------

function DeltaBadge({ value, suffix = '', invert = false }: { value: number | null | undefined; suffix?: string; invert?: boolean }) {
  if (value == null) return <span className="text-gray-400 text-sm">—</span>;
  const positive = invert ? value < 0 : value > 0;
  const negative = invert ? value > 0 : value < 0;
  const color = positive ? 'text-emerald-600 bg-emerald-50' : negative ? 'text-red-600 bg-red-50' : 'text-gray-600 bg-gray-50';
  const sign = value > 0 ? '+' : '';
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${color}`}>
      {positive && <TrendingUp size={12} />}
      {negative && <TrendingDown size={12} />}
      {!positive && !negative && <Minus size={12} />}
      {sign}{typeof value === 'number' ? (Number.isInteger(value) ? value : value.toFixed(2)) : value}{suffix}
    </span>
  );
}

function SentimentBadge({ sentiment }: { sentiment: string }) {
  const map: Record<string, { color: string; icon: React.ReactNode; label: string }> = {
    improving: { color: 'bg-emerald-100 text-emerald-800 border-emerald-200', icon: <TrendingUp size={16} />, label: 'Improving' },
    declining: { color: 'bg-red-100 text-red-800 border-red-200', icon: <TrendingDown size={16} />, label: 'Declining' },
    mixed: { color: 'bg-amber-100 text-amber-800 border-amber-200', icon: <Minus size={16} />, label: 'Mixed' },
  };
  const s = map[sentiment] || map.mixed;
  return (
    <span className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-sm font-semibold border ${s.color}`}>
      {s.icon} {s.label}
    </span>
  );
}

function MetricCard({ label, current, baseline, delta, pctChange, invert = false }: {
  label: string;
  current?: any;
  baseline?: any;
  delta?: number | null;
  pctChange?: number | null;
  invert?: boolean;
}) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="text-xs text-gray-500 uppercase tracking-wide mb-2">{label}</div>
      <div className="flex items-end justify-between gap-2">
        <div>
          <div className="text-2xl font-bold text-gray-900">
            {current != null ? (typeof current === 'number' ? current.toLocaleString() : String(current)) : '—'}
          </div>
          {baseline != null && (
            <div className="text-xs text-gray-400 mt-0.5">
              was {typeof baseline === 'number' ? baseline.toLocaleString() : String(baseline)}
            </div>
          )}
        </div>
        <div className="text-right">
          {delta != null && <DeltaBadge value={delta} invert={invert} />}
          {pctChange != null && (
            <div className="text-[10px] text-gray-400 mt-0.5">{pctChange > 0 ? '+' : ''}{pctChange.toFixed(1)}%</div>
          )}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Module delta renderers
// ---------------------------------------------------------------------------

const MODULE_ICONS: Record<number, string> = {
  1: '📈', 2: '🔍', 3: '🌐', 4: '📝', 5: '🎯', 6: '🤖',
  7: '🧭', 8: '📊', 9: '🏗️', 10: '🏷️', 11: '🛡️', 12: '💰',
};

function ModuleDeltaSection({ delta, isOpen, onToggle }: { delta: ModuleDelta; isOpen: boolean; onToggle: () => void }) {
  const icon = MODULE_ICONS[delta.module] || '📋';

  return (
    <div className="border border-gray-200 rounded-lg overflow-hidden">
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between px-5 py-4 bg-white hover:bg-gray-50 transition-colors text-left"
      >
        <div className="flex items-center gap-3">
          <span className="text-xl">{icon}</span>
          <div>
            <div className="font-semibold text-gray-900">Module {delta.module}: {delta.name}</div>
            <ModuleSummaryLine delta={delta} />
          </div>
        </div>
        {isOpen ? <ChevronUp size={18} className="text-gray-400" /> : <ChevronDown size={18} className="text-gray-400" />}
      </button>

      {isOpen && (
        <div className="px-5 py-4 bg-gray-50 border-t border-gray-200">
          <ModuleDeltaDetail delta={delta} />
        </div>
      )}
    </div>
  );
}

function ModuleSummaryLine({ delta }: { delta: ModuleDelta }) {
  const metrics = delta.metrics || delta;
  // Try to extract key delta values for a one-line summary
  const keyChanges: string[] = [];

  // Module 1
  if (metrics.overall_direction?.changed) {
    keyChanges.push(`Direction: ${metrics.overall_direction.baseline} → ${metrics.overall_direction.current}`);
  }
  // Module 2
  if (metrics.critical_pages?.delta != null) {
    const d = metrics.critical_pages.delta;
    keyChanges.push(`Critical pages: ${d > 0 ? '+' : ''}${d}`);
  }
  // Module 12
  if (delta.total_revenue?.delta != null) {
    const d = delta.total_revenue.delta;
    keyChanges.push(`Revenue: ${d > 0 ? '+' : ''}$${Math.abs(d).toLocaleString()}`);
  }

  if (keyChanges.length === 0) return null;
  return <div className="text-xs text-gray-500 mt-0.5">{keyChanges.join(' · ')}</div>;
}

function ModuleDeltaDetail({ delta }: { delta: ModuleDelta }) {
  const metrics = delta.metrics || {};

  // Generic renderer: render all metric objects that have current/baseline/delta
  const metricEntries = Object.entries(metrics).filter(
    ([, v]) => v && typeof v === 'object' && ('current' in v || 'delta' in v)
  );

  // Also check top-level keys on delta itself (some modules put metrics at top level)
  const topLevelMetrics = Object.entries(delta).filter(
    ([k, v]) => !['module', 'name', 'metrics'].includes(k) && v && typeof v === 'object' && ('current' in v || 'delta' in v)
  );

  const allMetrics = [...metricEntries, ...topLevelMetrics];

  if (allMetrics.length === 0) {
    return <div className="text-sm text-gray-500 italic">No detailed delta data available for this module.</div>;
  }

  return (
    <div className="space-y-4">
      {/* Metric cards grid */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {allMetrics.map(([key, val]: [string, any]) => {
          const label = key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
          // Handle "changed" boolean type (like direction)
          if (val.changed !== undefined && typeof val.current === 'string') {
            return (
              <div key={key} className="bg-white rounded-lg border border-gray-200 p-4">
                <div className="text-xs text-gray-500 uppercase tracking-wide mb-2">{label}</div>
                <div className="flex items-center gap-2">
                  <span className="text-sm text-gray-500">{val.baseline || '—'}</span>
                  <span className="text-gray-400">→</span>
                  <span className={`text-sm font-semibold ${val.changed ? 'text-blue-600' : 'text-gray-700'}`}>{val.current || '—'}</span>
                </div>
              </div>
            );
          }
          return (
            <MetricCard
              key={key}
              label={label}
              current={val.current}
              baseline={val.baseline}
              delta={val.delta}
              pctChange={val.pct_change}
            />
          );
        })}
      </div>

      {/* Lists: added/removed/changed items if present */}
      {renderListComparison(delta, 'competitors')}
      {renderListComparison(delta, 'keywords')}
      {renderListComparison(delta, 'pages')}

      {/* Bucket migration for Module 2 */}
      {delta.bucket_migration && (
        <div className="mt-3">
          <h4 className="text-sm font-semibold text-gray-700 mb-2">Page Bucket Migration</h4>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
            {Object.entries(delta.bucket_migration as Record<string, any>).map(([bucket, data]: [string, any]) => (
              <div key={bucket} className="bg-white border rounded-md p-3 text-center">
                <div className="text-xs text-gray-500 capitalize">{bucket}</div>
                <div className="text-lg font-bold">{data?.current ?? '—'}</div>
                {data?.delta != null && <DeltaBadge value={data.delta} invert={bucket === 'critical' || bucket === 'decaying'} />}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recovered / new critical pages for Module 2 */}
      {delta.new_critical_pages?.length > 0 && (
        <div className="mt-3">
          <h4 className="text-sm font-semibold text-red-700 mb-1">New Critical Pages</h4>
          <ul className="text-sm text-gray-700 space-y-1">
            {delta.new_critical_pages.slice(0, 10).map((p: string, i: number) => (
              <li key={i} className="font-mono text-xs bg-red-50 px-2 py-1 rounded">{p}</li>
            ))}
          </ul>
        </div>
      )}
      {delta.recovered_pages?.length > 0 && (
        <div className="mt-3">
          <h4 className="text-sm font-semibold text-emerald-700 mb-1">Recovered Pages</h4>
          <ul className="text-sm text-gray-700 space-y-1">
            {delta.recovered_pages.slice(0, 10).map((p: string, i: number) => (
              <li key={i} className="font-mono text-xs bg-emerald-50 px-2 py-1 rounded">{p}</li>
            ))}
          </ul>
        </div>
      )}

      {/* Algorithm updates for Module 6 */}
      {delta.new_impacting_updates?.length > 0 && (
        <div className="mt-3">
          <h4 className="text-sm font-semibold text-amber-700 mb-1">New Algorithm Impacts</h4>
          <div className="space-y-1">
            {delta.new_impacting_updates.map((u: any, i: number) => (
              <div key={i} className="text-sm bg-amber-50 px-3 py-2 rounded border border-amber-100">
                {typeof u === 'string' ? u : u.update_name || JSON.stringify(u)}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Revenue top keywords for Module 12 */}
      {delta.top_keywords_revenue?.changed?.length > 0 && (
        <div className="mt-3">
          <h4 className="text-sm font-semibold text-gray-700 mb-2">Top Keyword Revenue Changes</h4>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs text-gray-500 border-b">
                  <th className="pb-1">Keyword</th>
                  <th className="pb-1 text-right">Current</th>
                  <th className="pb-1 text-right">Baseline</th>
                  <th className="pb-1 text-right">Change</th>
                </tr>
              </thead>
              <tbody>
                {delta.top_keywords_revenue.changed.slice(0, 10).map((kw: any, i: number) => (
                  <tr key={i} className="border-b border-gray-100">
                    <td className="py-1 font-mono text-xs">{kw.keyword || kw.query || '—'}</td>
                    <td className="py-1 text-right">${(kw.current || 0).toLocaleString()}</td>
                    <td className="py-1 text-right text-gray-400">${(kw.baseline || 0).toLocaleString()}</td>
                    <td className="py-1 text-right"><DeltaBadge value={kw.delta} /></td>
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

function renderListComparison(delta: any, key: string) {
  const data = delta[key];
  if (!data || typeof data !== 'object') return null;
  if (!data.added_count && !data.removed_count && !data.changed_count) return null;

  const label = key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

  return (
    <div className="mt-3">
      <h4 className="text-sm font-semibold text-gray-700 mb-2">{label} Changes</h4>
      <div className="flex gap-3 text-xs mb-2">
        {data.added_count > 0 && <span className="px-2 py-0.5 bg-emerald-50 text-emerald-700 rounded">+{data.added_count} new</span>}
        {data.removed_count > 0 && <span className="px-2 py-0.5 bg-red-50 text-red-700 rounded">-{data.removed_count} removed</span>}
        {data.changed_count > 0 && <span className="px-2 py-0.5 bg-blue-50 text-blue-700 rounded">{data.changed_count} changed</span>}
      </div>
      {data.changed?.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-gray-500 border-b">
                <th className="pb-1">Item</th>
                <th className="pb-1 text-right">Current</th>
                <th className="pb-1 text-right">Baseline</th>
                <th className="pb-1 text-right">Delta</th>
              </tr>
            </thead>
            <tbody>
              {data.changed.slice(0, 8).map((item: any, i: number) => {
                const itemKey = Object.keys(item).find(k => !['current', 'baseline', 'delta', 'pct_change'].includes(k));
                return (
                  <tr key={i} className="border-b border-gray-100">
                    <td className="py-1 font-mono text-xs">{itemKey ? item[itemKey] : '—'}</td>
                    <td className="py-1 text-right">{item.current}</td>
                    <td className="py-1 text-right text-gray-400">{item.baseline}</td>
                    <td className="py-1 text-right"><DeltaBadge value={item.delta} /></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page Component
// ---------------------------------------------------------------------------

export default function ComparePage() {
  const router = useRouter();
  const { current, baseline } = router.query;

  const [authenticated, setAuthenticated] = useState<boolean | null>(null);
  const [history, setHistory] = useState<ReportHistoryItem[]>([]);
  const [loadingHistory, setLoadingHistory] = useState(true);
  const [selectedCurrent, setSelectedCurrent] = useState<string>('');
  const [selectedBaseline, setSelectedBaseline] = useState<string>('');
  const [comparison, setComparison] = useState<ComparisonResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [openModules, setOpenModules] = useState<Set<number>>(new Set());

  // Check auth status via cookie-based session (matches index.tsx pattern)
  useEffect(() => {
    fetch(`${API_BASE}/api/auth/status`, { credentials: 'include' })
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(data => {
        setAuthenticated(!!data.authenticated);
      })
      .catch(() => {
        setAuthenticated(false);
      });
  }, []);

  // Pre-fill from URL params
  useEffect(() => {
    if (current && typeof current === 'string') setSelectedCurrent(current);
    if (baseline && typeof baseline === 'string') setSelectedBaseline(baseline);
  }, [current, baseline]);

  // Fetch report history once authenticated
  useEffect(() => {
    if (!authenticated) return;
    setLoadingHistory(true);
    fetch(`${API_BASE}/api/v1/reports/user/history?limit=20`, {
      credentials: 'include',
    })
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(data => {
        setHistory(data);
        // Auto-select if only one pair or URL params present
        if (data.length >= 2 && !selectedCurrent && !selectedBaseline) {
          setSelectedCurrent(data[0].id);
          setSelectedBaseline(data[1].id);
        }
      })
      .catch(e => setError(`Failed to load report history: ${e.message}`))
      .finally(() => setLoadingHistory(false));
  }, [authenticated]);

  // Auto-run comparison if both params come from URL
  useEffect(() => {
    if (current && baseline && authenticated && selectedCurrent && selectedBaseline) {
      runComparison();
    }
  }, [selectedCurrent, selectedBaseline, authenticated]);

  const runComparison = useCallback(async () => {
    if (!selectedCurrent || !selectedBaseline || !authenticated) return;
    if (selectedCurrent === selectedBaseline) {
      setError('Please select two different reports to compare.');
      return;
    }
    setLoading(true);
    setError(null);
    setComparison(null);

    try {
      const res = await fetch(
        `${API_BASE}/api/v1/reports/${selectedCurrent}/compare?baseline_id=${selectedBaseline}`,
        { credentials: 'include' },
      );
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail || `HTTP ${res.status}`);
      }
      const data: ComparisonResult = await res.json();
      setComparison(data);
      // Expand first 3 modules by default
      setOpenModules(new Set(data.module_deltas.slice(0, 3).map(d => d.module)));
    } catch (e: any) {
      setError(e.message || 'Comparison failed');
    } finally {
      setLoading(false);
    }
  }, [selectedCurrent, selectedBaseline, authenticated]);

  const toggleModule = (mod: number) => {
    setOpenModules(prev => {
      const next = new Set(prev);
      next.has(mod) ? next.delete(mod) : next.add(mod);
      return next;
    });
  };

  const expandAll = () => {
    if (comparison) setOpenModules(new Set(comparison.module_deltas.map(d => d.module)));
  };
  const collapseAll = () => setOpenModules(new Set());

  const formatDate = (iso: string) => {
    try {
      return new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' });
    } catch { return iso; }
  };

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  if (authenticated === null) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <Loader2 size={24} className="animate-spin text-gray-400" />
      </div>
    );
  }

  if (!authenticated) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h1 className="text-2xl font-bold text-gray-900 mb-2">Not Authenticated</h1>
          <p className="text-gray-600 mb-4">Please sign in to compare reports.</p>
          <Link href="/" className="text-blue-600 hover:underline">Go to Login</Link>
        </div>
      </div>
    );
  }

  return (
    <>
      <Head>
        <title>Compare Reports — Search Intelligence</title>
        <meta name="description" content="Compare two Search Intelligence Reports to see what changed" />
      </Head>

      <div className="min-h-screen bg-gray-50">
        <NavHeader activePage="compare" />

        <main className="max-w-7xl mx-auto px-4 sm:px-6 py-8">
          {/* Report Picker */}
          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-8">
            <h2 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-4">
              Select Reports to Compare
            </h2>

            {loadingHistory ? (
              <div className="flex items-center gap-2 text-gray-500">
                <Loader2 size={16} className="animate-spin" /> Loading report history…
              </div>
            ) : history.length < 2 ? (
              <div className="text-gray-500">
                You need at least 2 completed reports to compare. <Link href="/" className="text-blue-600 hover:underline">Generate a report</Link> first.
              </div>
            ) : (
              <div className="flex flex-col sm:flex-row items-start sm:items-end gap-4">
                <div className="flex-1 w-full">
                  <label className="block text-xs text-gray-500 mb-1">Baseline (older)</label>
                  <select
                    value={selectedBaseline}
                    onChange={e => setSelectedBaseline(e.target.value)}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  >
                    <option value="">Select baseline report…</option>
                    {history.map(r => (
                      <option key={r.id} value={r.id} disabled={r.id === selectedCurrent}>
                        {r.domain} — {formatDate(r.created_at)}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="text-gray-400 text-lg font-bold hidden sm:block pb-2">→</div>
                <div className="flex-1 w-full">
                  <label className="block text-xs text-gray-500 mb-1">Current (newer)</label>
                  <select
                    value={selectedCurrent}
                    onChange={e => setSelectedCurrent(e.target.value)}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  >
                    <option value="">Select current report…</option>
                    {history.map(r => (
                      <option key={r.id} value={r.id} disabled={r.id === selectedBaseline}>
                        {r.domain} — {formatDate(r.created_at)}
                      </option>
                    ))}
                  </select>
                </div>
                <button
                  onClick={runComparison}
                  disabled={!selectedCurrent || !selectedBaseline || loading || selectedCurrent === selectedBaseline}
                  className="px-5 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 whitespace-nowrap"
                >
                  {loading ? <Loader2 size={16} className="animate-spin" /> : <GitCompare size={16} />}
                  Compare
                </button>
              </div>
            )}

            {error && (
              <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700 flex items-center gap-2">
                <AlertTriangle size={16} /> {error}
              </div>
            )}
          </div>

          {/* Comparison Results */}
          {comparison && (
            <div className="space-y-6">
              {/* Executive Summary */}
              <div className="bg-white rounded-xl border border-gray-200 p-6">
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <h2 className="text-lg font-bold text-gray-900">Executive Summary</h2>
                    <p className="text-sm text-gray-500">
                      {comparison.metadata.current_domain} · {formatDate(comparison.metadata.baseline_created_at)} → {formatDate(comparison.metadata.current_created_at)}
                    </p>
                  </div>
                  <SentimentBadge sentiment={comparison.executive_summary.overall_sentiment} />
                </div>

                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                  <div className="text-center p-4 rounded-lg bg-gray-50">
                    <div className="text-3xl font-bold text-gray-900">{comparison.modules_compared}</div>
                    <div className="text-xs text-gray-500 mt-1">Modules Compared</div>
                  </div>
                  <div className="text-center p-4 rounded-lg bg-emerald-50">
                    <div className="text-3xl font-bold text-emerald-700">{comparison.executive_summary.total_highlights}</div>
                    <div className="text-xs text-emerald-600 mt-1">Improvements</div>
                  </div>
                  <div className="text-center p-4 rounded-lg bg-red-50">
                    <div className="text-3xl font-bold text-red-700">{comparison.executive_summary.total_warnings}</div>
                    <div className="text-xs text-red-600 mt-1">Warnings</div>
                  </div>
                </div>

                {/* Highlights */}
                {comparison.executive_summary.highlights.length > 0 && (
                  <div className="mb-4">
                    <h3 className="text-sm font-semibold text-emerald-700 flex items-center gap-1.5 mb-2">
                      <CheckCircle size={14} /> Highlights
                    </h3>
                    <div className="space-y-2">
                      {comparison.executive_summary.highlights.map((h, i) => (
                        <div key={i} className="flex gap-3 p-3 bg-emerald-50 rounded-lg border border-emerald-100">
                          <div className="flex-1">
                            <div className="text-sm font-medium text-emerald-900">{h.signal}</div>
                            <div className="text-xs text-emerald-700 mt-0.5">{h.detail}</div>
                          </div>
                          <div className="text-[10px] text-emerald-500 whitespace-nowrap">{h.module}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Warnings */}
                {comparison.executive_summary.warnings.length > 0 && (
                  <div>
                    <h3 className="text-sm font-semibold text-red-700 flex items-center gap-1.5 mb-2">
                      <AlertTriangle size={14} /> Warnings
                    </h3>
                    <div className="space-y-2">
                      {comparison.executive_summary.warnings.map((w, i) => (
                        <div key={i} className="flex gap-3 p-3 bg-red-50 rounded-lg border border-red-100">
                          <div className="flex-1">
                            <div className="text-sm font-medium text-red-900">{w.signal}</div>
                            <div className="text-xs text-red-700 mt-0.5">{w.detail}</div>
                          </div>
                          <div className="text-[10px] text-red-500 whitespace-nowrap">{w.module}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>

              {/* Module Deltas */}
              <div className="bg-white rounded-xl border border-gray-200 p-6">
                <div className="flex items-center justify-between mb-4">
                  <h2 className="text-lg font-bold text-gray-900">Module-by-Module Changes</h2>
                  <div className="flex gap-2">
                    <button onClick={expandAll} className="text-xs text-blue-600 hover:underline">Expand All</button>
                    <span className="text-gray-300">|</span>
                    <button onClick={collapseAll} className="text-xs text-blue-600 hover:underline">Collapse All</button>
                  </div>
                </div>

                <div className="space-y-3">
                  {comparison.module_deltas
                    .sort((a, b) => a.module - b.module)
                    .map(delta => (
                      <ModuleDeltaSection
                        key={delta.module}
                        delta={delta}
                        isOpen={openModules.has(delta.module)}
                        onToggle={() => toggleModule(delta.module)}
                      />
                    ))
                  }
                </div>

                {comparison.modules_missing.length > 0 && (
                  <div className="mt-4 p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">
                    Modules {comparison.modules_missing.join(', ')} had no data in one or both reports and were skipped.
                  </div>
                )}
              </div>

              {/* Consulting CTA */}
              <div className="bg-gradient-to-r from-blue-600 to-indigo-700 rounded-xl p-8 text-white text-center">
                <h2 className="text-xl font-bold mb-2">Want help acting on these changes?</h2>
                <p className="text-blue-100 mb-4 max-w-xl mx-auto">
                  Our search consultants can help you capitalize on improvements and address declines before they impact revenue.
                </p>
                <a
                  href="https://clankermarketing.com/contact"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-2 px-6 py-3 bg-white text-blue-700 font-semibold rounded-lg hover:bg-blue-50 transition-colors"
                >
                  Book a Strategy Call
                </a>
              </div>
            </div>
          )}
        </main>
      </div>

      <style jsx global>{`
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
      `}</style>
    </>
  );
}
