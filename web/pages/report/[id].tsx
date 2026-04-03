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

// Import Module1TrafficOverview
import Module1TrafficOverview from '../../components/Module1TrafficOverview';

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
  RadarChart,
  Radar as RechartsRadar,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
} from 'recharts';
import { Menu, X, ChevronDown, ChevronUp, ExternalLink, Download, Calendar, TrendingUp, TrendingDown, Minus, AlertTriangle, CheckCircle, Target, Zap, RefreshCw, Clock, BarChart2, Shield, Globe, DollarSign, Search, FileText, Activity, Layers, Users, Mail, Loader2, Check } from 'lucide-react';
import NavHeader from '../../components/NavHeader';
import ErrorBoundary from '../../components/ErrorBoundary';

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
  revenue_forecast:     { title: 'Revenue Forecast',        icon: 'dollar-sign', number: 12 },
};

// ---------------------------------------------------------------------------
// Type definitions
// ---------------------------------------------------------------------------
interface ReportJob {
  id: string;
  user_id: string;
  site_domain: string;
  status: string;
  progress: number;
  result: ReportResult | null;
  error_message?: string;
  created_at: string;
  updated_at: string;
}

interface ReportResult {
  modules: Record<string, any>;
  executive_summary?: {
    overall_health: string;
    key_findings: string[];
    top_recommendations: string[];
  };
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------
function getStatusColor(status: string): string {
  switch (status) {
    case 'completed':
    case 'complete':
      return 'text-green-400';
    case 'partial':
      return 'text-yellow-400';
    case 'failed':
      return 'text-red-400';
    case 'processing':
    case 'pending':
      return 'text-blue-400';
    default:
      return 'text-slate-400';
  }
}

function getStatusIcon(status: string) {
  switch (status) {
    case 'completed':
    case 'complete':
      return <Check className="w-5 h-5" />;
    case 'partial':
      return <AlertTriangle className="w-5 h-5" />;
    case 'failed':
      return <X className="w-5 h-5" />;
    case 'processing':
      return <Loader2 className="w-5 h-5 animate-spin" />;
    default:
      return <Clock className="w-5 h-5" />;
  }
}

function formatDate(dateStr: string): string {
  try {
    return new Date(dateStr).toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  } catch {
    return dateStr;
  }
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------
export default function ReportDetailPage() {
  const router = useRouter();
  const { id } = router.query;
  
  const [job, setJob] = useState<ReportJob | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set());
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const pollTimerRef = useRef<NodeJS.Timeout | null>(null);

  // ---------------------------------------------------------------------------
  // Fetch report job
  // ---------------------------------------------------------------------------
  const fetchJob = useCallback(async () => {
    if (!id || typeof id !== 'string') return;
    
    try {
      const res = await fetch(`${API_BASE}/api/reports/${id}`);
      if (!res.ok) {
        if (res.status === 404) {
          throw new Error('Report not found');
        }
        throw new Error(`Failed to fetch report: ${res.statusText}`);
      }
      const data = await res.json();
      setJob(data);
      setError(null);
      
      // Stop polling if terminal status reached
      if (TERMINAL_STATUSES.has(data.status)) {
        if (pollTimerRef.current) {
          clearInterval(pollTimerRef.current);
          pollTimerRef.current = null;
        }
      }
    } catch (err) {
      console.error('Error fetching report:', err);
      setError(err instanceof Error ? err.message : 'Failed to load report');
      if (pollTimerRef.current) {
        clearInterval(pollTimerRef.current);
        pollTimerRef.current = null;
      }
    } finally {
      setLoading(false);
    }
  }, [id]);

  // ---------------------------------------------------------------------------
  // Initial load and polling setup
  // ---------------------------------------------------------------------------
  useEffect(() => {
    if (!id) return;
    
    fetchJob();
    
    // Start polling if not already terminal
    if (!job || !TERMINAL_STATUSES.has(job.status)) {
      pollTimerRef.current = setInterval(fetchJob, POLL_INTERVAL_MS);
    }
    
    return () => {
      if (pollTimerRef.current) {
        clearInterval(pollTimerRef.current);
        pollTimerRef.current = null;
      }
    };
  }, [id, fetchJob, job?.status]);

  // ---------------------------------------------------------------------------
  // Section toggle
  // ---------------------------------------------------------------------------
  const toggleSection = (moduleKey: string) => {
    setExpandedSections((prev) => {
      const next = new Set(prev);
      if (next.has(moduleKey)) {
        next.delete(moduleKey);
      } else {
        next.add(moduleKey);
      }
      return next;
    });
  };

  // ---------------------------------------------------------------------------
  // Download report as JSON
  // ---------------------------------------------------------------------------
  const downloadReport = () => {
    if (!job?.result) return;
    
    const dataStr = JSON.stringify(job.result, null, 2);
    const blob = new Blob([dataStr], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `report-${job.site_domain}-${job.id}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  // ---------------------------------------------------------------------------
  // Render loading state
  // ---------------------------------------------------------------------------
  if (loading) {
    return (
      <>
        <Head>
          <title>Loading Report | Search Intelligence</title>
        </Head>
        <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
          <NavHeader />
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
            <div className="flex flex-col items-center justify-center py-24">
              <Loader2 className="w-12 h-12 text-blue-400 animate-spin mb-4" />
              <p className="text-slate-300 text-lg">Loading report...</p>
            </div>
          </div>
        </div>
      </>
    );
  }

  // ---------------------------------------------------------------------------
  // Render error state
  // ---------------------------------------------------------------------------
  if (error || !job) {
    return (
      <>
        <Head>
          <title>Report Error | Search Intelligence</title>
        </Head>
        <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
          <NavHeader />
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
            <div className="flex flex-col items-center justify-center py-24">
              <AlertTriangle className="w-16 h-16 text-red-400 mb-4" />
              <h1 className="text-2xl font-bold text-slate-200 mb-2">Error Loading Report</h1>
              <p className="text-slate-400 mb-6">{error || 'Report not found'}</p>
              <button
                onClick={() => router.push('/dashboard')}
                className="px-6 py-3 bg-blue-500 hover:bg-blue-600 text-white rounded-lg transition-colors"
              >
                Back to Dashboard
              </button>
            </div>
          </div>
        </div>
      </>
    );
  }

  // ---------------------------------------------------------------------------
  // Render processing state
  // ---------------------------------------------------------------------------
  if (job.status === 'pending' || job.status === 'processing') {
    return (
      <>
        <Head>
          <title>Generating Report | Search Intelligence</title>
        </Head>
        <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
          <NavHeader />
          <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
            <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl border border-slate-700/50 p-8">
              <div className="flex items-center justify-between mb-6">
                <div>
                  <h1 className="text-2xl font-bold text-slate-200 mb-1">
                    Generating Report
                  </h1>
                  <p className="text-slate-400">{job.site_domain}</p>
                </div>
                <Loader2 className="w-8 h-8 text-blue-400 animate-spin" />
              </div>
              
              <div className="space-y-4">
                <div className="flex items-center justify-between text-sm">
                  <span className="text-slate-400">Progress</span>
                  <span className="text-slate-300 font-medium">{job.progress}%</span>
                </div>
                
                <div className="w-full bg-slate-700 rounded-full h-3 overflow-hidden">
                  <div
                    className="h-full bg-gradient-to-r from-blue-500 to-cyan-500 rounded-full transition-all duration-500 ease-out"
                    style={{ width: `${job.progress}%` }}
                  />
                </div>
                
                <p className="text-slate-400 text-sm mt-6">
                  This typically takes 2-5 minutes. We're analyzing your site's search performance across 12 comprehensive modules.
                </p>
                
                <div className="flex items-center text-sm text-slate-500 mt-4">
                  <Clock className="w-4 h-4 mr-2" />
                  Started {formatDate(job.created_at)}
                </div>
              </div>
            </div>
          </div>
        </div>
      </>
    );
  }

  // ---------------------------------------------------------------------------
  // Render failed state
  // ---------------------------------------------------------------------------
  if (job.status === 'failed') {
    return (
      <>
        <Head>
          <title>Report Failed | Search Intelligence</title>
        </Head>
        <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
          <NavHeader />
          <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
            <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl border border-red-500/30 p-8">
              <div className="flex items-start space-x-4">
                <AlertTriangle className="w-8 h-8 text-red-400 flex-shrink-0 mt-1" />
                <div className="flex-1">
                  <h1 className="text-2xl font-bold text-slate-200 mb-2">
                    Report Generation Failed
                  </h1>
                  <p className="text-slate-400 mb-4">{job.site_domain}</p>
                  
                  {job.error_message && (
                    <div className="bg-red-900/20 border border-red-500/30 rounded-lg p-4 mb-6">
                      <p className="text-red-300 text-sm">{job.error_message}</p>
                    </div>
                  )}
                  
                  <div className="flex items-center space-x-4">
                    <button
                      onClick={() => router.push('/dashboard')}
                      className="px-6 py-3 bg-slate-700 hover:bg-slate-600 text-white rounded-lg transition-colors"
                    >
                      Back to Dashboard
                    </button>
                    <button
                      onClick={() => window.location.reload()}
                      className="px-6 py-3 bg-blue-500 hover:bg-blue-600 text-white rounded-lg transition-colors flex items-center"
                    >
                      <RefreshCw className="w-4 h-4 mr-2" />
                      Retry
                    </button>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </>
    );
  }

  // ---------------------------------------------------------------------------
  // Render completed report
  // ---------------------------------------------------------------------------
  const modules = job.result?.modules || {};
  const executiveSummary = job.result?.executive_summary;

  return (
    <>
      <Head>
        <title>{job.site_domain} Report | Search Intelligence</title>
        <meta name="description" content={`Search Intelligence Report for ${job.site_domain}`} />
      </Head>

      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
        <NavHeader />

        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          {/* Report header */}
          <div className="mb-8">
            <div className="flex items-start justify-between mb-4">
              <div>
                <h1 className="text-3xl font-bold text-slate-200 mb-2">
                  Search Intelligence Report
                </h1>
                <div className="flex items-center space-x-4 text-sm text-slate-400">
                  <span className="flex items-center">
                    <Globe className="w-4 h-4 mr-1.5" />
                    {job.site_domain}
                  </span>
                  <span className="flex items-center">
                    <Calendar className="w-4 h-4 mr-1.5" />
                    {formatDate(job.created_at)}
                  </span>
                  <span className={`flex items-center ${getStatusColor(job.status)}`}>
                    {getStatusIcon(job.status)}
                    <span className="ml-1.5 capitalize">{job.status}</span>
                  </span>
                </div>
              </div>
              
              <button
                onClick={downloadReport}
                className="px-4 py-2 bg-slate-700 hover:bg-slate-600 text-white rounded-lg transition-colors flex items-center text-sm"
              >
                <Download className="w-4 h-4 mr-2" />
                Download JSON
              </button>
            </div>

            {/* Executive summary */}
            {executiveSummary && (
              <div className="bg-gradient-to-br from-blue-900/20 to-cyan-900/20 backdrop-blur-sm rounded-xl border border-blue-500/30 p-6 mb-6">
                <h2 className="text-xl font-semibold text-slate-200 mb-4 flex items-center">
                  <Target className="w-5 h-5 mr-2 text-blue-400" />
                  Executive Summary
                </h2>
                
                {executiveSummary.overall_health && (
                  <div className="mb-4">
                    <span className="text-sm text-slate-400">Overall Health: </span>
                    <span className="text-slate-200 font-medium">{executiveSummary.overall_health}</span>
                  </div>
                )}
                
                {executiveSummary.key_findings && executiveSummary.key_findings.length > 0 && (
                  <div className="mb-4">
                    <h3 className="text-sm font-medium text-slate-300 mb-2">Key Findings</h3>
                    <ul className="space-y-1.5">
                      {executiveSummary.key_findings.map((finding, idx) => (
                        <li key={idx} className="flex items-start text-sm text-slate-300">
                          <CheckCircle className="w-4 h-4 mr-2 text-green-400 flex-shrink-0 mt-0.5" />
                          {finding}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                
                {executiveSummary.top_recommendations && executiveSummary.top_recommendations.length > 0 && (
                  <div>
                    <h3 className="text-sm font-medium text-slate-300 mb-2">Top Recommendations</h3>
                    <ul className="space-y-1.5">
                      {executiveSummary.top_recommendations.map((rec, idx) => (
                        <li key={idx} className="flex items-start text-sm text-slate-300">
                          <Zap className="w-4 h-4 mr-2 text-yellow-400 flex-shrink-0 mt-0.5" />
                          {rec}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Module sections */}
          <div className="space-y-6">
            {Object.entries(modules).map(([moduleKey, moduleData]) => {
              const meta = MODULE_META[moduleKey];
              const isExpanded = expandedSections.has(moduleKey);
              
              if (!meta) return null;

              return (
                <ErrorBoundary key={moduleKey}>
                  <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl border border-slate-700/50 overflow-hidden">
                    {/* Module header */}
                    <button
                      onClick={() => toggleSection(moduleKey)}
                      className="w-full px-6 py-4 flex items-center justify-between hover:bg-slate-700/30 transition-colors"
                    >
                      <div className="flex items-center space-x-3">
                        <div className="w-8 h-8 rounded-lg bg-blue-500/20 flex items-center justify-center text-blue-400 font-semibold text-sm">
                          {meta.number}
                        </div>
                        <h2 className="text-xl font-semibold text-slate-200">
                          {meta.title}
                        </h2>
                      </div>
                      {isExpanded ? (
                        <ChevronUp className="w-5 h-5 text-slate-400" />
                      ) : (
                        <ChevronDown className="w-5 h-5 text-slate-400" />
                      )}
                    </button>

                    {/* Module content */}
                    {isExpanded && (
                      <div className="px-6 py-6 border-t border-slate-700/50">
                        {moduleKey === 'health_trajectory' ? (
                          <ErrorBoundary>
                            <Module1TrafficOverview data={moduleData} />
                          </ErrorBoundary>
                        ) : (
                          <div className="prose prose-invert max-w-none">
                            <pre className="bg-slate-900/50 p-4 rounded-lg overflow-x-auto text-xs text-slate-300">
                              {JSON.stringify(moduleData, null, 2)}
                            </pre>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                </ErrorBoundary>
              );
            })}
          </div>

          {/* Empty state if no modules */}
          {Object.keys(modules).length === 0 && (
            <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl border border-slate-700/50 p-12 text-center">
              <FileText className="w-12 h-12 text-slate-600 mx-auto mb-4" />
              <h3 className="text-lg font-medium text-slate-400 mb-2">No Analysis Data</h3>
              <p className="text-slate-500">
                This report completed but contains no module data.
              </p>
            </div>
          )}
        </div>
      </div>
    </>
  );
}
