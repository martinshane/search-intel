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
// Utility components — dark theme
// ---------------------------------------------------------------------------

function DeltaBadge({ value, suffix = '', invert = false }: { value: number | null | undefined; suffix?: string; invert?: boolean }) {
  if (value == null) return <span className="text-slate-500 text-sm">—</span>;
  const positive = invert ? value < 0 : value > 0;
  const negative = invert ? value > 0 : value < 0;
  const color = positive ? 'text-emerald-400 bg-emerald-900/40' : negative ? 'text-red-400 bg-red-900/40' : 'text-slate-400 bg-slate-700/50';
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
    improving: { color: 'bg-emerald-900/40 text-emerald-300 border-emerald-700/50', icon: <TrendingUp size={16} />, label: 'Improving' },
    declining: { color: 'bg-red-900/40 text-red-300 border-red-700/50', icon: <TrendingDown size={16} />, label: 'Declining' },
    mixed: { color: 'bg-amber-900/40 text-amber-300 border-amber-700/50', icon: <Minus size={16} />, label: 'Mixed' },
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
    <div className="bg-slate-700/40 rounded-lg border border-slate-600/40 p-4">
      <div className="text-xs text-slate-400 uppercase tracking-wide mb-2">{label}</div>
      <div className="flex items-end justify-between gap-2">
        <div>
          <div className="text-2xl font-bold text-white">
            {current != null ? (typeof current === 'number' ? current.toLocaleString() : String(current)) : '—'}
          </div>
          {baseline != null && (
            <div className="text-xs text-slate-500 mt-0.5">
              was {typeof baseline === 'number' ? baseline.toLocaleString() : String(baseline)}
            </div>
          )}
        </div>
        <div className="text-right">
          {delta != null && <DeltaBadge value={delta} invert={invert} />}
          {pctChange != null && (
            <div className="text-[10px] text-slate-500 mt-0.5">{pctChange > 0 ? '+' : ''}{pctChange.toFixed(1)}%</div>
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
    <div className="border border-slate-700/50 rounded-lg overflow-hidden">
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between px-5 py-4 bg-slate-800/60 hover:bg-slate-800/80 transition-colors text-left"
      >
        <div className="flex items-center gap-3">
          <span className="text-xl">{icon}</span>
          <div>
            <div className="font-semibold text-white">Module {delta.module}: {delta.name}</div>
            <ModuleSummaryLine delta={delta} />
          </div>
        </div>
        {isOpen ? <ChevronUp size={18} className="text-slate-500" /> : <ChevronDown size={18} className="text-slate-500" />}
      </button>

      {isOpen && (
        <div className="px-5 py-4 bg-slate-800/30 border-t border-slate-700/50">
          <ModuleDeltaDetail delta={delta} />
        </div>
      )}
    </div>
  );
}

function ModuleSummaryLine({ delta }: { delta: ModuleDelta }) {
  const metrics = delta.metrics || delta;
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
  return <div className="text-xs text-slate-400 mt-0.5">{keyChanges.join(' · ')}</div>;
}

function ModuleDeltaDetail({ delta }: { delta: ModuleDelta }) {
  const metrics = delta.metrics || {};

  const metricEntries = Object.entries(metrics).filter(
    ([, v]) => v && typeof v === 'object' && ('current' in v || 'delta' in v)
  );

  const topLevelMetrics = Object.entries(delta).filter(
    ([k, v]) => !['module', 'name', 'metrics'].includes(k) && v && typeof v === 'object' && ('current' in v || 'delta' in v)
  );

  const allMetrics = [...metricEntries, ...topLevelMetrics];

  if (allMetrics.length === 0) {
    return <div className="text-sm text-slate-500 italic">No detailed delta data available for this module.</div>;
  }

  return (
    <div className="space-y-4">
      {/* Metric cards grid */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {allMetrics.map(([key, val]: [string, any]) => {
          const label = key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
          if (val.changed !== undefined && typeof val.current === 'string') {
            return (
              <div key={key} className="bg-slate-700/40 rounded-lg border border-slate-600/40 p-4">
                <div className="text-xs text-slate-400 uppercase tracking-wide mb-2">{label}</div>
                <div className="flex items-center gap-2">
                  <span className="text-sm text-slate-500">{val.baseline || '—'}</span>
                  <span className="text-slate-600">→</span>
                  <span className={`text-sm font-semibold ${val.changed ? 'text-blue-400' : 'text-slate-300'}`}>{val.current || '—'}</span>
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

      {/* Lists: added/removed/changed items */}
      {renderListComparison(delta, 'competitors')}
      {renderListComparison(delta, 'keywords')}
      {renderListComparison(delta, 'pages')}

      {/* Bucket migration for Module 2 */}
      {delta.bucket_migration && (
        <div className="mt-3">
          <h4 className="text-sm font-semibold text-slate-300 mb-2">Page Bucket Migration</h4>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
            {Object.entries(delta.bucket_migration as Record<string, any>).map(([bucket, data]: [string, any]) => (
              <div key={bucket} className="bg-slate-700/40 border border-slate-600/40 rounded-md p-3 text-center">
                <div className="text-xs text-slate-400 capitalize">{bucket}</div>
                <div className="text-lg font-bold text-white">{data?.current ?? '—'}</div>
                {data?.delta != null && <DeltaBadge value={data.delta} invert={bucket === 'critical' || bucket === 'decaying'} />}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recovered / new critical pages for Module 2 */}
      {delta.new_critical_pages?.length > 0 && (
        <div className="mt-3">
          <h4 className="text-sm font-semibold text-red-400 mb-1">New Critical Pages</h4>
          <ul className="text-sm text-slate-300 space-y-1">
            {delta.new_critical_pages.slice(0, 10).map((p: string, i: number) => (
              <li key={i} className="font-mono text-xs bg-red-900/30 px-2 py-1 rounded">{p}</li>
            ))}
          </ul>
        </div>
      )}
      {delta.recovered_pages?.length > 0 && (
        <div className="mt-3">
          <h4 className="text-sm font-semibold text-emerald-400 mb-1">Recovered Pages</h4>
          <ul className="text-sm text-slate-300 space-y-1">
            {delta.recovered_pages.slice(0, 10).map((p: string, i: number) => (
              <li key={i} className="font-mono text-xs bg-emerald-900/30 px-2 py-1 rounded">{p}</li>
            ))}
          </ul>
        </div>
      )}

      {/* Algorithm updates for Module 6 */}
      {delta.new_impacting_updates?.length > 0 && (
        <div className="mt-3">
          <h4 className="text-sm font-semibold text-amber-400 mb-1">New Algorithm Impacts</h4>
          <div className="space-y-1">
            {delta.new_impacting_updates.map((u: any, i: number) => (
              <div key={i} className="text-sm bg-amber-900/30 px-3 py-2 rounded border border-amber-700/30">
                <span className="text-amber-200">{typeof u === 'string' ? u : u.update_name || JSON.stringify(u)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Revenue top keywords for Module 12 */}
      {delta.top_keywords_revenue?.changed?.length > 0 && (
        <div className="mt-3">
          <h4 className="text-sm font-semibold text-slate-300 mb-2">Top Keyword Revenue Changes</h4>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs text-slate-400 border-b border-slate-700/50">
                  <th className="pb-1">Keyword</th>
                  <th className="pb-1 text-right">Current</th>
                  <th className="pb-1 text-right">Baseline</th>
                  <th className="pb-1 text-right">Change</th>
                </tr>
              </thead>
              <tbody>
                {delta.top_keywords_revenue.changed.slice(0, 10).map((kw: any, i: number) => (
                  <tr key={i} className="border-b border-slate-700/30">
                    <td className="py-1 font-mono text-xs text-slate-300">{kw.keyword || kw.query || '—'}</td>
                    <td className="py-1 text-right text-slate-200">${(kw.current || 0).toLocaleString()}</td>
                    <td className="py-1 text-right text-slate-500">${(kw.baseline || 0).toLocaleString()}</td>
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
      <h4 className="text-sm font-semibold text-slate-300 mb-2">{label} Changes</h4>
      <div className="flex gap-3 text-xs mb-2">
        {data.added_count > 0 && <span className="px-2 py-0.5 bg-emerald-900/40 text-emerald-400 rounded">+{data.added_count} new</span>}
        {data.removed_count > 0 && <span className="px-2 py-0.5 bg-red-900/40 text-red-400 rounded">-{data.removed_count} removed</span>}
        {data.changed_count > 0 && <span className="px-2 py-0.5 bg-blue-900/40 text-blue-400 rounded">{data.changed_count} changed</span>}
      </div>
      {data.changed?.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-slate-400 border-b border-slate-700/50">
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
                  <tr key={i} className="border-b border-slate-700/30">
                    <td className="py-1 font-mono text-xs text-slate-300">{itemKey ? item[itemKey] : '—'}</td>
                    <td className="py-1 text-right text-slate-200">{item.current}</td>
                    <td className="py-1 text-right text-slate-500">{item.baseline}</td>
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
    fetch(`${API_BASE}/api/reports/user/history?limit=20`, {
      credentials: 'include',
    })
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(data => {
        setHistory(data);
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
        `${API_BASE}/api/reports/${selectedCurrent}/compare?baseline_id=${selectedBaseline}`,
        { credentials: 'include' },
      );
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail || `HTTP ${res.status}`);
      }
      const data: ComparisonResult = await res.json();
      setComparison(data);
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
      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center">
        <Loader2 size={24} className="animate-spin text-slate-400" />
      </div>
    );
  }

  if (!authenticated) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center">
        <div className="text-center">
          <h1 className="text-2xl font-bold text-white mb-2">Not Authenticated</h1>
          <p className="text-slate-400 mb-4">Please sign in to compare reports.</p>
          <Link href="/" className="text-blue-400 hover:underline">Go to Login</Link>
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

      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
        <NavHeader activePage="compare" />

        <main className="max-w-7xl mx-auto px-4 sm:px-6 py-8">
          {/* Report Picker */}
          <div className="bg-slate-800/60 rounded-xl border border-slate-700/50 p-6 mb-8">
            <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wide mb-4">
              Select Reports to Compare
            </h2>

            {loadingHistory ? (
              <div className="flex items-center gap-2 text-slate-400">
                <Loader2 size={16} className="animate-spin" /> Loading report history…
              </div>
            ) : history.length < 2 ? (
              <div className="text-slate-400">
                You need at least 2 completed reports to compare. <Link href="/" className="text-blue-400 hover:underline">Generate a report</Link> first.
              </div>
            ) : (
              <div className="flex flex-col sm:flex-row items-start sm:items-end gap-4">
                <div className="flex-1 w-full">
                  <label className="block text-xs text-slate-400 mb-1">Baseline (older)</label>
                  <select
                    value={selectedBaseline}
                    onChange={e => setSelectedBaseline(e.target.value)}
                    className="w-full bg-slate-700/60 border border-slate-600/50 text-slate-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  >
                    <option value="">Select baseline report…</option>
                    {history.map(r => (
                      <option key={r.id} value={r.id} disabled={r.id === selectedCurrent}>
                        {r.domain} — {formatDate(r.created_at)}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="text-slate-500 text-lg font-bold hidden sm:block pb-2">→</div>
                <div className="flex-1 w-full">
                  <label className="block text-xs text-slate-400 mb-1">Current (newer)</label>
                  <select
                    value={selectedCurrent}
                    onChange={e => setSelectedCurrent(e.target.value)}
                    className="w-full bg-slate-700/60 border border-slate-600/50 text-slate-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
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
              <div className="mt-4 p-3 bg-red-900/30 border border-red-700/50 rounded-lg text-sm text-red-300 flex items-center gap-2">
                <AlertTriangle size={16} /> {error}
              </div>
            )}
          </div>

          {/* Comparison Results */}
          {comparison && (
            <div className="space-y-6">
              {/* Executive Summary */}
              <div className="bg-slate-800/60 rounded-xl border border-slate-700/50 p-6">
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <h2 className="text-lg font-bold text-white">Executive Summary</h2>
                    <p className="text-sm text-slate-400">
                      {comparison.metadata.current_domain} · {formatDate(comparison.metadata.baseline_created_at)} → {formatDate(comparison.metadata.current_created_at)}
                    </p>
                  </div>
                  <SentimentBadge sentiment={comparison.executive_summary.overall_sentiment} />
                </div>

                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                  <div className="text-center p-4 rounded-lg bg-slate-700/40">
                    <div className="text-3xl font-bold text-white">{comparison.modules_compared}</div>
                    <div className="text-xs text-slate-400 mt-1">Modules Compared</div>
                  </div>
                  <div className="text-center p-4 rounded-lg bg-emerald-900/30">
                    <div className="text-3xl font-bold text-emerald-400">{comparison.executive_summary.total_highlights}</div>
                    <div className="text-xs text-emerald-400/70 mt-1">Improvements</div>
                  </div>
                  <div className="text-center p-4 rounded-lg bg-red-900/30">
                    <div className="text-3xl font-bold text-red-400">{comparison.executive_summary.total_warnings}</div>
                    <div className="text-xs text-red-400/70 mt-1">Warnings</div>
                  </div>
                </div>

                {/* Highlights */}
                {comparison.executive_summary.highlights.length > 0 && (
                  <div className="mb-4">
                    <h3 className="text-sm font-semibold text-emerald-400 flex items-center gap-1.5 mb-2">
                      <CheckCircle size={14} /> Highlights
                    </h3>
                    <div className="space-y-2">
                      {comparison.executive_summary.highlights.map((h, i) => (
                        <div key={i} className="flex gap-3 p-3 bg-emerald-900/20 rounded-lg border border-emerald-700/30">
                          <div className="flex-1">
                            <div className="text-sm font-medium text-emerald-300">{h.signal}</div>
                            <div className="text-xs text-emerald-400/70 mt-0.5">{h.detail}</div>
                          </div>
                          <div className="text-[10px] text-emerald-500/60 whitespace-nowrap">{h.module}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Warnings */}
                {comparison.executive_summary.warnings.length > 0 && (
                  <div>
                    <h3 className="text-sm font-semibold text-red-400 flex items-center gap-1.5 mb-2">
                      <AlertTriangle size={14} /> Warnings
                    </h3>
                    <div className="space-y-2">
                      {comparison.executive_summary.warnings.map((w, i) => (
                        <div key={i} className="flex gap-3 p-3 bg-red-900/20 rounded-lg border border-red-700/30">
                          <div className="flex-1">
                            <div className="text-sm font-medium text-red-300">{w.signal}</div>
                            <div className="text-xs text-red-400/70 mt-0.5">{w.detail}</div>
                          </div>
                          <div className="text-[10px] text-red-500/60 whitespace-nowrap">{w.module}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>

              {/* Module Deltas */}
              <div className="bg-slate-800/60 rounded-xl border border-slate-700/50 p-6">
                <div className="flex items-center justify-between mb-4">
                  <h2 className="text-lg font-bold text-white">Module-by-Module Changes</h2>
                  <div className="flex gap-2">
                    <button onClick={expandAll} className="text-xs text-blue-400 hover:underline">Expand All</button>
                    <span className="text-slate-600">|</span>
                    <button onClick={collapseAll} className="text-xs text-blue-400 hover:underline">Collapse All</button>
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
                  <div className="mt-4 p-3 bg-amber-900/30 border border-amber-700/40 rounded-lg text-sm text-amber-300">
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
    </>
  );
}
