import React, { useState, useEffect, useCallback } from 'react';
import Head from 'next/head';
import Link from 'next/link';
import NavHeader from '../../components/NavHeader';
import { useRouter } from 'next/router';
import {
  BarChart2,
  Clock,
  ExternalLink,
  FileText,
  Plus,
  RefreshCw,
  Search,
  TrendingUp,
  Calendar,
  CheckCircle,
  AlertTriangle,
  Loader,
  ChevronRight,
  Globe,
  Activity,
} from 'lucide-react';

const API_BASE = process.env.NEXT_PUBLIC_API_URL || '';

/* ── Types ── */

interface Report {
  id: string;
  domain: string;
  status: string;
  created_at: string;
  completed_at?: string | null;
  current_module?: number | null;
  progress?: Record<string, string> | null;
}

interface UserInfo {
  email: string;
  gsc_connected: boolean;
  ga4_connected: boolean;
  gsc_properties: { id: string; name: string }[];
  ga4_properties: { id: string; name: string }[];
}

/* ── Helpers ── */

function formatDate(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

function relativeTime(iso: string): string {
  const now = Date.now();
  const then = new Date(iso).getTime();
  const diffMs = now - then;
  const diffMin = Math.floor(diffMs / 60000);
  const diffHr = Math.floor(diffMin / 60);
  const diffDay = Math.floor(diffHr / 24);

  if (diffMin < 1) return 'just now';
  if (diffMin < 60) return `${diffMin}m ago`;
  if (diffHr < 24) return `${diffHr}h ago`;
  if (diffDay < 7) return `${diffDay}d ago`;
  return formatDate(iso);
}

function StatusBadge({ status }: { status: string }) {
  const config: Record<string, { color: string; icon: React.ReactNode; label: string }> = {
    completed: {
      color: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30',
      icon: <CheckCircle className="w-3.5 h-3.5" />,
      label: 'Completed',
    },
    complete: {
      color: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30',
      icon: <CheckCircle className="w-3.5 h-3.5" />,
      label: 'Completed',
    },
    partial: {
      color: 'bg-amber-500/20 text-amber-300 border-amber-500/30',
      icon: <AlertTriangle className="w-3.5 h-3.5" />,
      label: 'Partial',
    },
    failed: {
      color: 'bg-red-500/20 text-red-300 border-red-500/30',
      icon: <AlertTriangle className="w-3.5 h-3.5" />,
      label: 'Failed',
    },
    running: {
      color: 'bg-blue-500/20 text-blue-300 border-blue-500/30',
      icon: <Loader className="w-3.5 h-3.5 animate-spin" />,
      label: 'Running',
    },
    analyzing: {
      color: 'bg-blue-500/20 text-blue-300 border-blue-500/30',
      icon: <Loader className="w-3.5 h-3.5 animate-spin" />,
      label: 'Analyzing',
    },
    ingesting: {
      color: 'bg-indigo-500/20 text-indigo-300 border-indigo-500/30',
      icon: <Loader className="w-3.5 h-3.5 animate-spin" />,
      label: 'Ingesting Data',
    },
    pending: {
      color: 'bg-slate-500/20 text-slate-300 border-slate-500/30',
      icon: <Clock className="w-3.5 h-3.5" />,
      label: 'Pending',
    },
  };

  const c = config[status] || config.pending;

  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${c.color}`}>
      {c.icon}
      {c.label}
    </span>
  );
}

function ModuleProgress({ current, total = 12 }: { current: number | null | undefined; total?: number }) {
  if (!current || current < 1) return null;
  const pct = Math.min(100, Math.round((current / total) * 100));
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-1.5 bg-slate-700 rounded-full overflow-hidden">
        <div
          className="h-full bg-blue-500 rounded-full transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="text-xs text-slate-400 tabular-nums whitespace-nowrap">
        {current}/{total}
      </span>
    </div>
  );
}

/* ── Stats Card ── */

function StatCard({ icon, label, value, sub }: { icon: React.ReactNode; label: string; value: string | number; sub?: string }) {
  return (
    <div className="bg-slate-800/60 border border-slate-700/50 rounded-xl p-5">
      <div className="flex items-center gap-3 mb-3">
        <div className="p-2 rounded-lg bg-slate-700/50 text-slate-300">{icon}</div>
        <span className="text-sm text-slate-400">{label}</span>
      </div>
      <p className="text-2xl font-bold text-white">{value}</p>
      {sub && <p className="text-xs text-slate-500 mt-1">{sub}</p>}
    </div>
  );
}

/* ── Empty State ── */

function EmptyState() {
  return (
    <div className="text-center py-16 px-4">
      <div className="mx-auto w-16 h-16 rounded-full bg-slate-800 flex items-center justify-center mb-6">
        <Search className="w-8 h-8 text-slate-500" />
      </div>
      <h3 className="text-xl font-semibold text-white mb-2">No reports yet</h3>
      <p className="text-slate-400 max-w-md mx-auto mb-8">
        Generate your first Search Intelligence Report to see a comprehensive analysis of your
        site&apos;s search performance across 12 data-driven modules.
      </p>
      <Link
        href="/"
        className="inline-flex items-center gap-2 px-6 py-3 bg-blue-600 hover:bg-blue-500 text-white rounded-lg font-medium transition"
      >
        <Plus className="w-4 h-4" />
        Generate Your First Report
      </Link>
    </div>
  );
}

/* ── Main Page ── */

export default function MyReportsPage() {
  const router = useRouter();
  const [reports, setReports] = useState<Report[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [user, setUser] = useState<UserInfo | null>(null);

  const fetchAuthStatus = useCallback(async () => {
    try {
      const resp = await fetch(`${API_BASE}/api/auth/status`, { credentials: 'include' });
      if (resp.ok) {
        const data = await resp.json();
        setUser(data);
      }
    } catch {
      // Auth check is optional for page render
    }
  }, []);

  const fetchReports = useCallback(async () => {
    try {
      const resp = await fetch(`${API_BASE}/api/reports/user/me`, { credentials: 'include' });
      if (resp.status === 401) {
        // Not authenticated — redirect to connect page
        router.push('/');
        return;
      }
      if (!resp.ok) throw new Error('Failed to load reports');
      const data: Report[] = await resp.json();
      setReports(data);
    } catch (err: any) {
      setError(err.message || 'Something went wrong');
    } finally {
      setLoading(false);
    }
  }, [router]);

  useEffect(() => {
    fetchAuthStatus();
    fetchReports();
  }, [fetchAuthStatus, fetchReports]);

  // Auto-refresh running reports
  useEffect(() => {
    const hasRunning = reports.some((r) =>
      ['pending', 'running', 'analyzing', 'ingesting'].includes(r.status)
    );
    if (!hasRunning) return;
    const interval = setInterval(fetchReports, 10000);
    return () => clearInterval(interval);
  }, [reports, fetchReports]);

  // Stats
  const completedCount = reports.filter((r) => ['completed', 'complete'].includes(r.status)).length;
  const runningCount = reports.filter((r) => ['pending', 'running', 'analyzing', 'ingesting'].includes(r.status)).length;
  const uniqueDomains = new Set(reports.map((r) => r.domain)).size;
  const latestReport = reports.length > 0 ? reports[0] : null;

  return (
    <>
      <Head>
        <title>My Reports — Search Intelligence Report</title>
        <meta name="description" content="View and manage your Search Intelligence Reports" />
      </Head>

      <div className="min-h-screen bg-gradient-to-b from-slate-950 via-slate-900 to-slate-950 text-white">
        <NavHeader activePage="reports" />

        <main className="max-w-7xl mx-auto px-4 sm:px-6 py-8">
          {/* ── Page Title ── */}
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between mb-8 gap-4">
            <div>
              <h1 className="text-2xl sm:text-3xl font-bold text-white">My Reports</h1>
              <p className="text-slate-400 mt-1">
                {user?.email
                  ? `Signed in as ${user.email}`
                  : 'View and manage your search intelligence reports'}
              </p>
            </div>
            <button
              onClick={() => {
                setLoading(true);
                fetchReports();
              }}
              className="inline-flex items-center gap-2 px-4 py-2 bg-slate-800 hover:bg-slate-700 border border-slate-700 rounded-lg text-sm text-slate-300 transition"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
          </div>

          {/* ── Stats Row ── */}
          {!loading && reports.length > 0 && (
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
              <StatCard
                icon={<FileText className="w-5 h-5" />}
                label="Total Reports"
                value={reports.length}
              />
              <StatCard
                icon={<CheckCircle className="w-5 h-5" />}
                label="Completed"
                value={completedCount}
                sub={runningCount > 0 ? `${runningCount} in progress` : undefined}
              />
              <StatCard
                icon={<Globe className="w-5 h-5" />}
                label="Domains Analyzed"
                value={uniqueDomains}
              />
              <StatCard
                icon={<TrendingUp className="w-5 h-5" />}
                label="Latest"
                value={latestReport ? relativeTime(latestReport.created_at) : '—'}
                sub={latestReport?.domain}
              />
            </div>
          )}

          {/* ── Error ── */}
          {error && (
            <div className="bg-red-500/10 border border-red-500/30 rounded-xl p-4 mb-6 flex items-start gap-3">
              <AlertTriangle className="w-5 h-5 text-red-400 flex-shrink-0 mt-0.5" />
              <div>
                <p className="text-red-300 font-medium">Failed to load reports</p>
                <p className="text-red-400/70 text-sm mt-1">{error}</p>
              </div>
            </div>
          )}

          {/* ── Loading ── */}
          {loading && (
            <div className="flex items-center justify-center py-20">
              <Loader className="w-8 h-8 text-blue-500 animate-spin" />
            </div>
          )}

          {/* ── Empty ── */}
          {!loading && !error && reports.length === 0 && <EmptyState />}

          {/* ── Reports List ── */}
          {!loading && reports.length > 0 && (
            <div className="space-y-3">
              {reports.map((report) => {
                const isActive = ['pending', 'running', 'analyzing', 'ingesting'].includes(report.status);
                const isComplete = ['completed', 'complete'].includes(report.status);
                const isPartial = report.status === 'partial';

                return (
                  <div
                    key={report.id}
                    className={`group relative bg-slate-800/40 hover:bg-slate-800/70 border rounded-xl p-5 transition cursor-pointer ${
                      isActive
                        ? 'border-blue-500/30'
                        : isComplete
                        ? 'border-slate-700/50 hover:border-slate-600/50'
                        : 'border-slate-700/30'
                    }`}
                    onClick={() => router.push(`/reports/${report.id}`)}
                  >
                    <div className="flex flex-col sm:flex-row sm:items-center gap-4">
                      {/* Domain + date */}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-3 mb-1">
                          <Globe className="w-4 h-4 text-slate-500 flex-shrink-0" />
                          <h3 className="text-lg font-semibold text-white truncate">
                            {report.domain}
                          </h3>
                        </div>
                        <div className="flex items-center gap-4 text-sm text-slate-500">
                          <span className="flex items-center gap-1.5">
                            <Calendar className="w-3.5 h-3.5" />
                            {formatDate(report.created_at)}
                          </span>
                          {report.id && (
                            <span className="hidden sm:inline font-mono text-xs text-slate-600">
                              {report.id.substring(0, 8)}
                            </span>
                          )}
                        </div>
                      </div>

                      {/* Progress (for running reports) */}
                      {isActive && (
                        <div className="w-full sm:w-40">
                          <ModuleProgress current={report.current_module} />
                        </div>
                      )}

                      {/* Status + action */}
                      <div className="flex items-center gap-4">
                        <StatusBadge status={report.status} />
                        <ChevronRight className="w-5 h-5 text-slate-600 group-hover:text-slate-400 transition hidden sm:block" />
                      </div>
                    </div>

                    {/* Quick actions row */}
                    {(isComplete || isPartial) && (
                      <div className="flex items-center gap-3 mt-3 pt-3 border-t border-slate-700/30">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            router.push(`/reports/${report.id}`);
                          }}
                          className="inline-flex items-center gap-1.5 text-xs text-blue-400 hover:text-blue-300 transition"
                        >
                          <ExternalLink className="w-3.5 h-3.5" />
                          View Report
                        </button>
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            window.open(
                              `${API_BASE}/api/reports/${report.id}/pdf`,
                              '_blank'
                            );
                          }}
                          className="inline-flex items-center gap-1.5 text-xs text-slate-400 hover:text-slate-300 transition"
                        >
                          <FileText className="w-3.5 h-3.5" />
                          Download PDF
                        </button>
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            router.push(`/compare?report=${report.id}`);
                          }}
                          className="inline-flex items-center gap-1.5 text-xs text-slate-400 hover:text-slate-300 transition"
                        >
                          <BarChart2 className="w-3.5 h-3.5" />
                          Compare
                        </button>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {/* ── Bottom CTA ── */}
          {!loading && reports.length > 0 && (
            <div className="mt-12 bg-gradient-to-r from-blue-900/30 to-indigo-900/30 border border-blue-500/20 rounded-xl p-8 text-center">
              <h3 className="text-xl font-bold text-white mb-2">
                Need help interpreting your reports?
              </h3>
              <p className="text-slate-300 mb-6 max-w-lg mx-auto">
                Our search consultants can walk you through your findings and build a custom growth
                strategy for your site.
              </p>
              <a
                href="https://clankermarketing.com/contact"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-2 px-6 py-3 bg-blue-600 hover:bg-blue-500 text-white rounded-lg font-medium transition"
              >
                Book a Free Strategy Call
                <ExternalLink className="w-4 h-4" />
              </a>
            </div>
          )}
        </main>

        {/* ── Footer ── */}
        <footer className="border-t border-slate-800 mt-12">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 py-6 flex flex-col sm:flex-row items-center justify-between gap-4 text-sm text-slate-500">
            <p>
              &copy; {new Date().getFullYear()}{' '}
              <a
                href="https://clankermarketing.com"
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-500 hover:text-blue-400 transition"
              >
                Clanker Marketing
              </a>
            </p>
            <div className="flex items-center gap-6">
              <Link href="/" className="hover:text-white transition">
                Home
              </Link>
              <Link href="/reports" className="hover:text-white transition">
                Reports
              </Link>
              <Link href="/schedules" className="hover:text-white transition">
                Schedules
              </Link>
              <Link href="/progress" className="hover:text-white transition">
                System Status
              </Link>
            </div>
          </div>
        </footer>
      </div>
    </>
  );
}
