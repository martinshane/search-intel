import React, { useState, useEffect, useCallback } from 'react';
import { useRouter } from 'next/router';
import Head from 'next/head';
import Link from 'next/link';
import NavHeader from '../components/NavHeader';
import { FileText, RefreshCw, ExternalLink, Clock, CheckCircle, AlertTriangle, Loader, Trash2, ChevronRight, Plus } from 'lucide-react';

const API_BASE = process.env.NEXT_PUBLIC_API_URL || '';

interface Report {
  id: string;
  domain: string;
  status: string;
  created_at: string;
  current_module?: number;
  progress?: Record<string, string>;
}

const STATUS_CONFIG: Record<string, { label: string; color: string; bg: string; icon: React.ElementType }> = {
  completed: { label: 'Completed', color: 'text-green-700', bg: 'bg-green-50 border-green-200', icon: CheckCircle },
  complete:  { label: 'Completed', color: 'text-green-700', bg: 'bg-green-50 border-green-200', icon: CheckCircle },
  partial:   { label: 'Partial',   color: 'text-amber-700', bg: 'bg-amber-50 border-amber-200', icon: AlertTriangle },
  failed:    { label: 'Failed',    color: 'text-red-700',   bg: 'bg-red-50 border-red-200',     icon: AlertTriangle },
  queued:    { label: 'Queued',    color: 'text-blue-700',  bg: 'bg-blue-50 border-blue-200',   icon: Clock },
  ingesting: { label: 'Ingesting', color: 'text-blue-700',  bg: 'bg-blue-50 border-blue-200',   icon: Loader },
  analyzing: { label: 'Analyzing', color: 'text-blue-700',  bg: 'bg-blue-50 border-blue-200',   icon: Loader },
  running:   { label: 'Running',   color: 'text-blue-700',  bg: 'bg-blue-50 border-blue-200',   icon: Loader },
};

function StatusBadge({ status }: { status: string }) {
  const cfg = STATUS_CONFIG[status] || { label: status, color: 'text-gray-700', bg: 'bg-gray-50 border-gray-200', icon: Clock };
  const Icon = cfg.icon;
  const isRunning = ['queued', 'ingesting', 'analyzing', 'running'].includes(status);
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${cfg.bg} ${cfg.color}`}>
      <Icon className={`w-3.5 h-3.5 ${isRunning ? 'animate-spin' : ''}`} />
      {cfg.label}
    </span>
  );
}

function ModuleProgress({ progress, currentModule }: { progress?: Record<string, string>; currentModule?: number }) {
  if (!progress || Object.keys(progress).length === 0) return null;
  const total = 12;
  const completed = Object.values(progress).filter(s => s === 'success' || s === 'completed').length;
  const pct = Math.round((completed / total) * 100);
  return (
    <div className="mt-2">
      <div className="flex items-center justify-between text-xs text-slate-500 mb-1">
        <span>{completed}/{total} modules</span>
        <span>{pct}%</span>
      </div>
      <div className="h-1.5 bg-slate-200 rounded-full overflow-hidden">
        <div className="h-full bg-blue-500 rounded-full transition-all duration-500" style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function formatDate(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  } catch {
    return iso;
  }
}

function formatTime(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
  } catch {
    return '';
  }
}

export default function ReportsPage() {
  const router = useRouter();
  const [reports, setReports] = useState<Report[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [retrying, setRetrying] = useState<string | null>(null);

  const fetchReports = useCallback(async () => {
    try {
      const token = typeof window !== 'undefined' ? localStorage.getItem('auth_token') || '' : '';
      const headers: Record<string, string> = {};
      if (token) headers['Authorization'] = `Bearer ${token}`;

      const res = await fetch(`${API_BASE}/api/reports/user/me`, {
        headers,
        credentials: 'include',
      });

      if (res.status === 401) {
        router.push('/?auth=expired');
        return;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      const data = await res.json();
      setReports(data);
      setError(null);
    } catch (err: any) {
      setError(err.message || 'Failed to load reports');
    } finally {
      setLoading(false);
    }
  }, [router]);

  useEffect(() => {
    fetchReports();
    // Poll for running reports
    const interval = setInterval(fetchReports, 15000);
    return () => clearInterval(interval);
  }, [fetchReports]);

  const handleRetry = async (reportId: string) => {
    setRetrying(reportId);
    try {
      const token = typeof window !== 'undefined' ? localStorage.getItem('auth_token') || '' : '';
      const headers: Record<string, string> = { 'Content-Type': 'application/json' };
      if (token) headers['Authorization'] = `Bearer ${token}`;

      const res = await fetch(`${API_BASE}/api/reports/${reportId}/retry`, {
        method: 'POST',
        headers,
        credentials: 'include',
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || `HTTP ${res.status}`);
      }
      await fetchReports();
    } catch (err: any) {
      alert(err.message || 'Failed to retry report');
    } finally {
      setRetrying(null);
    }
  };

  const hasRunning = reports.some(r => ['queued', 'ingesting', 'analyzing', 'running'].includes(r.status));

  return (
    <>
      <Head>
        <title>My Reports — Search Intelligence</title>
      </Head>
      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
        <NavHeader activePage="reports" />

        <main className="max-w-4xl mx-auto px-4 sm:px-6 py-8">
          {/* Header */}
          <div className="flex items-center justify-between mb-8">
            <div>
              <h1 className="text-2xl font-bold text-white">My Reports</h1>
              <p className="text-sm text-slate-400 mt-1">
                {reports.length === 0 && !loading ? 'No reports yet' : `${reports.length} report${reports.length !== 1 ? 's' : ''}`}
              </p>
            </div>
            <Link
              href="/"
              className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition text-sm font-medium"
            >
              <Plus className="w-4 h-4" />
              New Report
            </Link>
          </div>

          {/* Error */}
          {error && (
            <div className="mb-6 p-4 bg-red-900/30 border border-red-700/50 rounded-lg text-red-300 text-sm">
              {error}
            </div>
          )}

          {/* Loading skeleton */}
          {loading && (
            <div className="space-y-4">
              {[1, 2, 3].map(i => (
                <div key={i} className="bg-slate-800/50 rounded-lg p-6 animate-pulse">
                  <div className="h-5 bg-slate-700 rounded w-48 mb-3" />
                  <div className="h-4 bg-slate-700 rounded w-32" />
                </div>
              ))}
            </div>
          )}

          {/* Empty state */}
          {!loading && reports.length === 0 && !error && (
            <div className="text-center py-16">
              <FileText className="w-16 h-16 text-slate-600 mx-auto mb-4" />
              <h2 className="text-xl font-semibold text-white mb-2">No reports yet</h2>
              <p className="text-slate-400 mb-6 max-w-md mx-auto">
                Connect your Google Search Console and GA4 accounts to generate your first Search Intelligence Report.
              </p>
              <Link
                href="/"
                className="inline-flex items-center gap-2 px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition font-medium"
              >
                <Plus className="w-5 h-5" />
                Generate Your First Report
              </Link>
            </div>
          )}

          {/* Report list */}
          {!loading && reports.length > 0 && (
            <div className="space-y-3">
              {reports.map(report => {
                const isRunning = ['queued', 'ingesting', 'analyzing', 'running'].includes(report.status);
                const isFailed = report.status === 'failed';
                const isComplete = ['completed', 'complete', 'partial'].includes(report.status);

                return (
                  <div
                    key={report.id}
                    className={`bg-slate-800/60 border rounded-lg hover:bg-slate-800/80 transition ${
                      isRunning ? 'border-blue-700/40' : isFailed ? 'border-red-700/30' : 'border-slate-700/50'
                    }`}
                  >
                    <div className="p-4 sm:p-5">
                      <div className="flex items-start justify-between gap-4">
                        <div className="min-w-0 flex-1">
                          {/* Domain + status */}
                          <div className="flex items-center gap-3 mb-1.5">
                            <h3 className="text-lg font-semibold text-white truncate">{report.domain}</h3>
                            <StatusBadge status={report.status} />
                          </div>

                          {/* Date */}
                          <div className="text-sm text-slate-400 flex items-center gap-2">
                            <Clock className="w-3.5 h-3.5" />
                            {formatDate(report.created_at)} at {formatTime(report.created_at)}
                          </div>

                          {/* Progress bar for running reports */}
                          {isRunning && (
                            <ModuleProgress progress={report.progress} currentModule={report.current_module} />
                          )}
                        </div>

                        {/* Actions */}
                        <div className="flex items-center gap-2 flex-shrink-0">
                          {isFailed && (
                            <button
                              onClick={(e) => { e.stopPropagation(); handleRetry(report.id); }}
                              disabled={retrying === report.id}
                              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-amber-400 border border-amber-700/40 rounded-lg hover:bg-amber-900/30 transition disabled:opacity-50"
                            >
                              <RefreshCw className={`w-3.5 h-3.5 ${retrying === report.id ? 'animate-spin' : ''}`} />
                              Retry
                            </button>
                          )}

                          {isComplete && (
                            <Link
                              href={`/report/${report.id}`}
                              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-blue-400 border border-blue-700/40 rounded-lg hover:bg-blue-900/30 transition"
                            >
                              View Report
                              <ChevronRight className="w-3.5 h-3.5" />
                            </Link>
                          )}

                          {isRunning && (
                            <Link
                              href={`/report/${report.id}`}
                              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-slate-400 border border-slate-600/40 rounded-lg hover:bg-slate-700/30 transition"
                            >
                              Watch Progress
                              <ChevronRight className="w-3.5 h-3.5" />
                            </Link>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </main>
      </div>
    </>
  );
}
