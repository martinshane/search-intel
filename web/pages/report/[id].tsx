import { useEffect, useState } from 'react';
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
import { Menu, X, ChevronDown, ChevronUp, ExternalLink, Download, Calendar, TrendingUp, TrendingDown, Minus, AlertTriangle, CheckCircle, Target, Zap } from 'lucide-react';

interface ReportData {
  id: string;
  user_id: string;
  gsc_property: string;
  ga4_property: string | null;
  status: string;
  progress: Record<string, string>;
  report_data: {
    health_trajectory?: any;
    page_triage?: any;
    serp_landscape?: any;
    content_intelligence?: any;
    gameplan?: any;
    algorithm_impact?: any;
    intent_migration?: any;
    ctr_modeling?: any;
    site_architecture?: any;
    branded_split?: any;
    competitive_threats?: any;
    revenue_attribution?: any;
  };
  created_at: string;
  completed_at: string | null;
}

export default function ReportPage() {
  const router = useRouter();
  const { id } = router.query;
  const [report, setReport] = useState<ReportData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(['health', 'gameplan']));
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  useEffect(() => {
    if (!id) return;

    const fetchReport = async () => {
      try {
        const response = await fetch(`/api/reports/${id}`);
        if (!response.ok) {
          throw new Error('Failed to fetch report');
        }
        const data = await response.json();
        setReport(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchReport();
  }, [id]);

  const toggleSection = (section: string) => {
    setExpandedSections((prev) => {
      const next = new Set(prev);
      if (next.has(section)) {
        next.delete(section);
      } else {
        next.add(section);
      }
      return next;
    });
  };

  const formatNumber = (num: number, decimals = 0): string => {
    return new Intl.NumberFormat('en-US', {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    }).format(num);
  };

  const formatCurrency = (num: number): string => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(num);
  };

  const formatDate = (dateStr: string): string => {
    return new Date(dateStr).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
    });
  };

  const getTrendIcon = (direction: string) => {
    switch (direction) {
      case 'strong_growth':
      case 'growth':
      case 'growing':
        return <TrendingUp className="w-5 h-5 text-green-500" />;
      case 'strong_decline':
      case 'decline':
      case 'declining':
        return <TrendingDown className="w-5 h-5 text-red-500" />;
      default:
        return <Minus className="w-5 h-5 text-gray-500" />;
    }
  };

  const getBucketColor = (bucket: string): string => {
    switch (bucket) {
      case 'growing':
        return '#10b981';
      case 'stable':
        return '#6b7280';
      case 'decaying':
        return '#f59e0b';
      case 'critical':
        return '#ef4444';
      default:
        return '#6b7280';
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading report...</p>
        </div>
      </div>
    );
  }

  if (error || !report) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center px-4">
        <div className="text-center">
          <AlertTriangle className="w-12 h-12 text-red-500 mx-auto mb-4" />
          <h1 className="text-2xl font-bold text-gray-900 mb-2">Report Not Found</h1>
          <p className="text-gray-600 mb-6">{error || 'The requested report could not be found.'}</p>
          <button
            onClick={() => router.push('/dashboard')}
            className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
          >
            Back to Dashboard
          </button>
        </div>
      </div>
    );
  }

  if (report.status !== 'complete') {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center px-4">
        <div className="text-center max-w-md">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <h1 className="text-2xl font-bold text-gray-900 mb-2">Report In Progress</h1>
          <p className="text-gray-600 mb-6">
            Your report is currently being generated. This typically takes 2-5 minutes.
          </p>
          <div className="bg-white rounded-lg p-6 mb-6">
            <h2 className="text-sm font-semibold text-gray-700 mb-3">Progress</h2>
            <div className="space-y-2 text-sm">
              {Object.entries(report.progress).map(([module, status]) => (
                <div key={module} className="flex items-center justify-between">
                  <span className="text-gray-600">{module.replace(/_/g, ' ')}</span>
                  <span className={`px-2 py-1 rounded text-xs font-medium ${
                    status === 'complete' ? 'bg-green-100 text-green-800' :
                    status === 'running' ? 'bg-blue-100 text-blue-800' :
                    'bg-gray-100 text-gray-600'
                  }`}>
                    {status}
                  </span>
                </div>
              ))}
            </div>
          </div>
          <button
            onClick={() => router.push('/dashboard')}
            className="text-blue-600 hover:text-blue-700 transition"
          >
            Back to Dashboard
          </button>
        </div>
      </div>
    );
  }

  const { report_data } = report;

  return (
    <>
      <Head>
        <title>Search Intelligence Report - {report.gsc_property}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      </Head>

      <div className="min-h-screen bg-gray-50">
        {/* Header */}
        <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="flex items-center justify-between h-16">
              <div className="flex-1 min-w-0">
                <h1 className="text-lg sm:text-xl font-bold text-gray-900 truncate">
                  {report.gsc_property}
                </h1>
                <p className="text-xs sm:text-sm text-gray-500 mt-1">
                  Generated {formatDate(report.created_at)}
                </p>
              </div>
              <div className="flex items-center space-x-2 sm:space-x-4 ml-4">
                <button
                  onClick={() => window.print()}
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

          {/* Mobile menu */}
          {mobileMenuOpen && (
            <div className="sm:hidden border-t border-gray-200 bg-white">
              <div className="px-4 py-3 space-y-2">
                <button
                  onClick={() => window.print()}
                  className="w-full flex items-center space-x-2 px-3 py-2 text-sm font-medium text-gray-700 bg-gray-50 rounded-lg hover:bg-gray-100 transition"
                >
                  <Download className="w-4 h-4" />
                  <span>Export PDF</span>
                </button>
                <button
                  onClick={() => router.push('/dashboard')}
                  className="w-full flex items-center space-x-2 px-3 py-2 text-sm font-medium text-gray-700 bg-gray-50 rounded-lg hover:bg-gray-100 transition"
                >
                  <span>Back to Dashboard</span>
                </button>
              </div>
            </div>
          )}
        </header>

        {/* Main content */}
        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 sm:py-8">
          <div className="space-y-6">
            {/* Section 1: Health & Trajectory */}
            {report_data.health_trajectory && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button
                  onClick={() => toggleSection('health')}
                  className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition"
                >
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    {getTrendIcon(report_data.health_trajectory.overall_direction)}
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">Health & Trajectory</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">
                        {report_data.health_trajectory.overall_direction === 'declining' ? 'Traffic declining' :
                         report_data.health_trajectory.overall_direction === 'growing' ? 'Traffic growing' :
                         'Traffic stable'} at {Math.abs(report_data.health_trajectory.trend_slope_pct_per_month || 0).toFixed(1)}%/month
                      </p>
                    </div>
                  </div>
                  {expandedSections.has('health') ? (
                    <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />
                  ) : (
                    <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />
                  )}
                </button>

                {expandedSections.has('health') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {report_data.health_trajectory.forecast && (
                      <div className="mb-6">
                        <h3 className="text-base sm:text-lg font-semibold text-gray-900 mb-4">Traffic Forecast</h3>
                        <div className="h-64 sm:h-80">
                          <ResponsiveContainer width="100%" height="100%">
                            <LineChart
                              data={[
                                { period: 'Current', clicks: report_data.health_trajectory.current_clicks || 0 },
                                { 
                                  period: '30d', 
                                  clicks: report_data.health_trajectory.forecast['30d']?.clicks || 0,
                                  ci_low: report_data.health_trajectory.forecast['30d']?.ci_low || 0,
                                  ci_high: report_data.health_trajectory.forecast['30d']?.ci_high || 0,
                                },
                                { 
                                  period: '60d', 
                                  clicks: report_data.health_trajectory.forecast['60d']?.clicks || 0,
                                  ci_low: report_data.health_trajectory.forecast['60d']?.ci_low || 0,
                                  ci_high: report_data.health_trajectory.forecast['60d']?.ci_high || 0,
                                },
                                { 
                                  period: '90d', 
                                  clicks: report_data.health_trajectory.forecast['90d']?.clicks || 0,
                                  ci_low: report_data.health_trajectory.forecast['90d']?.ci_low || 0,
                                  ci_high: report_data.health_trajectory.forecast['90d']?.ci_high || 0,
                                },
                              ]}
                              margin={{ top: 5, right: 10, left: 0, bottom: 5 }}
                            >
                              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                              <XAxis dataKey="period" tick={{ fontSize: 12 }} />
                              <YAxis tick={{ fontSize: 12 }} />
                              <Tooltip 
                                contentStyle={{ fontSize: '12px' }}
                                formatter={(value: number) => formatNumber(value)}
                              />
                              <Legend wrapperStyle={{ fontSize: '12px' }} />
                              <Line type="monotone" dataKey="clicks" stroke="#3b82f6" strokeWidth={2} dot={{ r: 4 }} />
                              <Line type="monotone" dataKey="ci_high" stroke="#93c5fd" strokeDasharray="5 5" dot={false} />
                              <Line type="monotone" dataKey="ci_low" stroke="#93c5fd" strokeDasharray="5 5" dot={false} />
                            </LineChart>
                          </ResponsiveContainer>
                        </div>
                      </div>
                    )}

                    {report_data.health_trajectory.seasonality && (
                      <div className="bg-blue-50 rounded-lg p-4 mb-6">
                        <h4 className="text-sm font-semibold text-gray-900 mb-2">Seasonality Patterns</h4>
                        <div className="text-sm text-gray-700 space-y-1">
                          <p>Best day: <span className="font-medium">{report_data.health_trajectory.seasonality.best_day}</span></p>
                          <p>Worst day: <span className="font-medium">{report_data.health_trajectory.seasonality.worst_day}</span></p>
                          {report_data.health_trajectory.seasonality.cycle_description && (
                            <p className="mt-2">{report_data.health_trajectory.seasonality.cycle_description}</p>
                          )}
                        </div>
                      </div>
                    )}

                    {report_data.health_trajectory.change_points && report_data.health_trajectory.change_points.length > 0 && (
                      <div>
                        <h4 className="text-sm font-semibold text-gray-900 mb-3">Significant Changes</h4>
                        <div className="overflow-x-auto -mx-4 sm:mx-0">
                          <div className="inline-block min-w-full align-middle">
                            <table className="min-w-full divide-y divide-gray-200">
                              <thead className="bg-gray-50">
                                <tr>
                                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Date</th>
                                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Direction</th>
                                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Magnitude</th>
                                </tr>
                              </thead>
                              <tbody className="bg-white divide-y divide-gray-200">
                                {report_data.health_trajectory.change_points.map((cp: any, idx: number) => (
                                  <tr key={idx}>
                                    <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">{formatDate(cp.date)}</td>
                                    <td className="px-4 py-3 whitespace-nowrap text-sm">
                                      <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                                        cp.direction === 'drop' ? 'bg-red-100 text-red-800' : 'bg-green-100 text-green-800'
                                      }`}>
                                        {cp.direction}
                                      </span>
                                    </td>
                                    <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">{(cp.magnitude * 100).toFixed(1)}%</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {/* Section 2: Page-Level Triage */}
            {report_data.page_triage && (
              <section className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <button
                  onClick={() => toggleSection('triage')}
                  className="w-full px-4 sm:px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition"
                >
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    <Target className="w-5 h-5 text-orange-500 flex-shrink-0" />
                    <div className="min-w-0 flex-1">
                      <h2 className="text-lg sm:text-xl font-bold text-gray-900">Page-Level Triage</h2>
                      <p className="text-sm text-gray-600 mt-1 truncate">
                        {report_data.page_triage.summary?.critical || 0} critical pages, {formatNumber(report_data.page_triage.summary?.total_recoverable_clicks_monthly || 0)} clicks recoverable
                      </p>
                    </div>
                  </div>
                  {expandedSections.has('triage') ? (
                    <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />
                  ) : (
                    <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0 ml-2" />
                  )}
                </button>

                {expandedSections.has('triage') && (
                  <div className="px-4 sm:px-6 py-4 sm:py-6 border-t border-gray-200">
                    {report_data.page_triage.summary && (
                      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 sm:gap-4 mb-6">
                        <div className="bg-green-50 rounded-lg p-3 sm:p-4">
                          <div className="text-2xl sm:text-3xl font-bold text-green-700">{report_data.page_triage.summary.growing || 0}</div>
                          <div className="text-xs sm:text-sm text-gray-600 mt-1">Growing</div>
                        </div>
                        <div className="bg-gray-50 rounded-lg p-3 sm:p-4">
                          <div className="text-2xl sm:text-3xl font-bold text-gray-700">{report_data.page_triage.summary.stable || 0}</div>
                          <div className="text-xs sm:text-sm text-gray-600 mt-1">Stable</div>
                        </div>
                        <div className="bg-orange-50 rounded-lg p-3 sm:p-4">
                          <div className="text-2xl sm:text-3xl font-bold text-orange-700">{report_data.page_triage.summary.decaying || 0}</div>
                          <div className="text-xs sm:text-sm text-gray-600 mt-1">Decaying</div>
                        </div>
                        <div className="bg-red-50 rounded-lg p-3 sm:p-4">
                          <div className="text-2xl sm:text-3xl font-bold text-red-700">{report_data.page_triage.summary.critical || 0}</div>
                          <div className="text-xs sm:text-sm text-gray-600 mt-1">Critical</div>
                        </div>
                      </div>
                    )}

                    {report_data