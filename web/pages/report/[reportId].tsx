import { useRouter } from 'next/router';
import { useEffect, useState } from 'react';
import Head from 'next/head';
import Module1TrafficOverview from '../../components/modules/Module1TrafficOverview';

interface ReportMetadata {
  id: string;
  site: string;
  date_range: {
    start: string;
    end: string;
  };
  generated_at: string;
  status: string;
}

interface Module1Data {
  overall_direction: string;
  trend_slope_pct_per_month: number;
  change_points: Array<{
    date: string;
    magnitude: number;
    direction: string;
  }>;
  seasonality: {
    best_day: string;
    worst_day: string;
    monthly_cycle: boolean;
    cycle_description: string;
  };
  anomalies: Array<{
    date: string;
    type: string;
    magnitude: number;
  }>;
  forecast: {
    '30d': {
      clicks: number;
      ci_low: number;
      ci_high: number;
    };
    '60d': {
      clicks: number;
      ci_low: number;
      ci_high: number;
    };
    '90d': {
      clicks: number;
      ci_low: number;
      ci_high: number;
    };
  };
}

interface Module2Data {
  pages: Array<{
    url: string;
    bucket: string;
    current_monthly_clicks: number;
    trend_slope: number;
    projected_page1_loss_date?: string;
    ctr_anomaly: boolean;
    ctr_expected?: number;
    ctr_actual?: number;
    engagement_flag?: string;
    priority_score: number;
    recommended_action: string;
  }>;
  summary: {
    total_pages_analyzed: number;
    growing: number;
    stable: number;
    decaying: number;
    critical: number;
    total_recoverable_clicks_monthly: number;
  };
}

interface Module3Data {
  keywords_analyzed: number;
  serp_feature_displacement: Array<{
    keyword: string;
    organic_position: number;
    visual_position: number;
    features_above: string[];
    estimated_ctr_impact: number;
  }>;
  competitors: Array<{
    domain: string;
    keywords_shared: number;
    avg_position: number;
    threat_level: string;
  }>;
  intent_mismatches: Array<any>;
  total_click_share: number;
  click_share_opportunity: number;
}

interface Module4Data {
  cannibalization_clusters: Array<{
    query_group: string;
    pages: string[];
    shared_queries: number;
    total_impressions_affected: number;
    recommendation: string;
    keep_page?: string;
  }>;
  striking_distance: Array<{
    query: string;
    current_position: number;
    impressions: number;
    estimated_click_gain_if_top5: number;
    intent: string;
    landing_page: string;
  }>;
  thin_content: Array<any>;
  update_priority_matrix: {
    urgent_update: Array<any>;
    leave_alone: Array<any>;
    structural_problem: Array<any>;
    double_down: Array<any>;
  };
}

interface Module5Data {
  critical: Array<{
    action: string;
    impact: number;
    effort: string;
    page?: string;
    keyword?: string;
    dependencies?: string[];
  }>;
  quick_wins: Array<any>;
  strategic: Array<any>;
  structural: Array<any>;
  total_estimated_monthly_click_recovery: number;
  total_estimated_monthly_click_growth: number;
  narrative: string;
}

interface ReportData {
  metadata: ReportMetadata;
  module_1?: Module1Data;
  module_2?: Module2Data;
  module_3?: Module3Data;
  module_4?: Module4Data;
  module_5?: Module5Data;
}

type TabType = 'overview' | 'pages' | 'serp' | 'content' | 'gameplan';

export default function ReportPage() {
  const router = useRouter();
  const { reportId } = router.query;
  
  const [report, setReport] = useState<ReportData | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<TabType>('overview');

  useEffect(() => {
    if (!reportId || typeof reportId !== 'string') {
      return;
    }

    const fetchReport = async () => {
      setLoading(true);
      setError(null);
      
      try {
        const response = await fetch(`/api/reports/${reportId}`);
        
        if (!response.ok) {
          if (response.status === 404) {
            throw new Error('Report not found');
          } else if (response.status === 500) {
            throw new Error('Server error while fetching report');
          } else {
            throw new Error(`Failed to fetch report: ${response.statusText}`);
          }
        }

        const data = await response.json();
        setReport(data);
      } catch (err) {
        console.error('Error fetching report:', err);
        setError(err instanceof Error ? err.message : 'An unexpected error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchReport();
  }, [reportId]);

  const formatDate = (dateString: string): string => {
    try {
      const date = new Date(dateString);
      return date.toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'long',
        day: 'numeric'
      });
    } catch {
      return dateString;
    }
  };

  const formatDateTime = (dateString: string): string => {
    try {
      const date = new Date(dateString);
      return date.toLocaleString('en-US', {
        year: 'numeric',
        month: 'long',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
      });
    } catch {
      return dateString;
    }
  };

  if (loading) {
    return (
      <>
        <Head>
          <title>Loading Report... | Search Intelligence Report</title>
        </Head>
        <div className="min-h-screen bg-gray-50 flex items-center justify-center">
          <div className="text-center">
            <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mb-4"></div>
            <h2 className="text-xl font-semibold text-gray-900 mb-2">Loading Report</h2>
            <p className="text-gray-600">Fetching your search intelligence data...</p>
          </div>
        </div>
      </>
    );
  }

  if (error) {
    return (
      <>
        <Head>
          <title>Error | Search Intelligence Report</title>
        </Head>
        <div className="min-h-screen bg-gray-50 flex items-center justify-center px-4">
          <div className="max-w-md w-full bg-white rounded-lg shadow-md p-8 text-center">
            <div className="text-red-600 mb-4">
              <svg className="w-16 h-16 mx-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              </svg>
            </div>
            <h2 className="text-2xl font-bold text-gray-900 mb-2">Error Loading Report</h2>
            <p className="text-gray-600 mb-6">{error}</p>
            <button
              onClick={() => router.push('/dashboard')}
              className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
            >
              <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
              </svg>
              Back to Dashboard
            </button>
          </div>
        </div>
      </>
    );
  }

  if (!report) {
    return (
      <>
        <Head>
          <title>Report Not Found | Search Intelligence Report</title>
        </Head>
        <div className="min-h-screen bg-gray-50 flex items-center justify-center px-4">
          <div className="max-w-md w-full bg-white rounded-lg shadow-md p-8 text-center">
            <h2 className="text-2xl font-bold text-gray-900 mb-2">Report Not Found</h2>
            <p className="text-gray-600 mb-6">The requested report could not be found.</p>
            <button
              onClick={() => router.push('/dashboard')}
              className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
            >
              Back to Dashboard
            </button>
          </div>
        </div>
      </>
    );
  }

  const { metadata } = report;
  const statusColors = {
    completed: 'bg-green-100 text-green-800',
    processing: 'bg-yellow-100 text-yellow-800',
    failed: 'bg-red-100 text-red-800',
    pending: 'bg-gray-100 text-gray-800'
  };

  const tabs = [
    { id: 'overview' as TabType, label: 'Traffic Overview', module: 'module_1', icon: '📊' },
    { id: 'pages' as TabType, label: 'Page Triage', module: 'module_2', icon: '📄' },
    { id: 'serp' as TabType, label: 'SERP Landscape', module: 'module_3', icon: '🔍' },
    { id: 'content' as TabType, label: 'Content Intelligence', module: 'module_4', icon: '📝' },
    { id: 'gameplan' as TabType, label: 'The Gameplan', module: 'module_5', icon: '🎯' }
  ];

  return (
    <>
      <Head>
        <title>{`${metadata.site} - Search Intelligence Report`}</title>
        <meta name="description" content={`Comprehensive search intelligence report for ${metadata.site}`} />
      </Head>

      <div className="min-h-screen bg-gray-50">
        {/* Header */}
        <header className="bg-white shadow-sm border-b border-gray-200">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-4">
                <button
                  onClick={() => router.push('/dashboard')}
                  className="text-gray-600 hover:text-gray-900 transition-colors"
                  aria-label="Back to Dashboard"
                >
                  <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
                  </svg>
                </button>
                <div>
                  <h1 className="text-2xl font-bold text-gray-900">{metadata.site}</h1>
                  <p className="text-sm text-gray-600">
                    {formatDate(metadata.date_range.start)} - {formatDate(metadata.date_range.end)}
                  </p>
                </div>
              </div>
              <div className="flex items-center space-x-3">
                <span className={`px-3 py-1 rounded-full text-sm font-medium ${statusColors[metadata.status as keyof typeof statusColors] || statusColors.pending}`}>
                  {metadata.status.charAt(0).toUpperCase() + metadata.status.slice(1)}
                </span>
                <span className="text-sm text-gray-500">
                  Generated {formatDateTime(metadata.generated_at)}
                </span>
              </div>
            </div>
          </div>
        </header>

        {/* Navigation Tabs */}
        <nav className="bg-white border-b border-gray-200">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="flex space-x-1 overflow-x-auto">
              {tabs.map((tab) => {
                const isAvailable = report[tab.module as keyof ReportData] !== undefined;
                const isActive = activeTab === tab.id;
                
                return (
                  <button
                    key={tab.id}
                    onClick={() => isAvailable && setActiveTab(tab.id)}
                    disabled={!isAvailable}
                    className={`
                      flex items-center space-x-2 px-4 py-3 border-b-2 font-medium text-sm whitespace-nowrap transition-colors
                      ${isActive
                        ? 'border-blue-600 text-blue-600'
                        : isAvailable
                          ? 'border-transparent text-gray-600 hover:text-gray-900 hover:border-gray-300'
                          : 'border-transparent text-gray-400 cursor-not-allowed'
                      }
                    `}
                  >
                    <span>{tab.icon}</span>
                    <span>{tab.label}</span>
                    {!isAvailable && (
                      <span className="text-xs bg-gray-200 text-gray-600 px-2 py-0.5 rounded">N/A</span>
                    )}
                  </button>
                );
              })}
            </div>
          </div>
        </nav>

        {/* Main Content */}
        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          {activeTab === 'overview' && report.module_1 && (
            <Module1TrafficOverview data={report.module_1} />
          )}

          {activeTab === 'pages' && report.module_2 && (
            <div className="space-y-6">
              <div className="bg-white rounded-lg shadow-md p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">Page-Level Triage</h2>
                <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
                  <div className="bg-blue-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-blue-600">{report.module_2.summary.total_pages_analyzed}</div>
                    <div className="text-sm text-gray-600">Total Pages</div>
                  </div>
                  <div className="bg-green-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-green-600">{report.module_2.summary.growing}</div>
                    <div className="text-sm text-gray-600">Growing</div>
                  </div>
                  <div className="bg-gray-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-gray-600">{report.module_2.summary.stable}</div>
                    <div className="text-sm text-gray-600">Stable</div>
                  </div>
                  <div className="bg-yellow-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-yellow-600">{report.module_2.summary.decaying}</div>
                    <div className="text-sm text-gray-600">Decaying</div>
                  </div>
                  <div className="bg-red-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-red-600">{report.module_2.summary.critical}</div>
                    <div className="text-sm text-gray-600">Critical</div>
                  </div>
                </div>
                <div className="bg-purple-50 p-4 rounded-lg mb-6">
                  <div className="text-lg font-semibold text-purple-900">
                    {report.module_2.summary.total_recoverable_clicks_monthly.toLocaleString()} clicks/month recoverable
                  </div>
                </div>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Page</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Monthly Clicks</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Trend</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Priority</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Action</th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {report.module_2.pages.slice(0, 20).map((page, idx) => (
                        <tr key={idx} className="hover:bg-gray-50">
                          <td className="px-4 py-3 text-sm text-gray-900 max-w-xs truncate">{page.url}</td>
                          <td className="px-4 py-3 text-sm">
                            <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                              page.bucket === 'growing' ? 'bg-green-100 text-green-800' :
                              page.bucket === 'stable' ? 'bg-gray-100 text-gray-800' :
                              page.bucket === 'decaying' ? 'bg-yellow-100 text-yellow-800' :
                              'bg-red-100 text-red-800'
                            }`}>
                              {page.bucket}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-sm text-gray-900">{page.current_monthly_clicks.toLocaleString()}</td>
                          <td className="px-4 py-3 text-sm text-gray-900">{page.trend_slope.toFixed(2)}</td>
                          <td className="px-4 py-3 text-sm text-gray-900">{page.priority_score.toFixed(1)}</td>
                          <td className="px-4 py-3 text-sm text-gray-600">{page.recommended_action.replace(/_/g, ' ')}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}

          {activeTab === 'serp' && report.module_3 && (
            <div className="space-y-6">
              <div className="bg-white rounded-lg shadow-md p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">SERP Landscape Analysis</h2>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                  <div className="bg-blue-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-blue-600">{report.module_3.keywords_analyzed}</div>
                    <div className="text-sm text-gray-600">Keywords Analyzed</div>
                  </div>
                  <div className="bg-purple-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-purple-600">{(report.module_3.total_click_share * 100).toFixed(1)}%</div>
                    <div className="text-sm text-gray-600">Click Share</div>
                  </div>
                  <div className="bg-green-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-green-600">{(report.module_3.click_share_opportunity * 100).toFixed(1)}%</div>
                    <div className="text-sm text-gray-600">Opportunity</div>
                  </div>
                  <div className="bg-yellow-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-yellow-600">{report.module_3.competitors.length}</div>
                    <div className="text-sm text-gray-600">Competitors</div>
                  </div>
                </div>

                <h3 className="text-lg font-semibold text-gray-900 mb-3">SERP Feature Displacement</h3>
                <div className="overflow-x-auto mb-6">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Keyword</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Organic Pos</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Visual Pos</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Features</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">CTR Impact</th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {report.module_3.serp_feature_displacement.map((item, idx) => (
                        <tr key={idx}>
                          <td className="px-4 py-3 text-sm text-gray-900">{item.keyword}</td>
                          <td className="px-4 py-3 text-sm text-gray-900">{item.organic_position}</td>
                          <td className="px-4 py-3 text-sm text-gray-900">{item.visual_position}</td>
                          <td className="px-4 py-3 text-sm text-gray-600">{item.features_above.join(', ')}</td>
                          <td className="px-4 py-3 text-sm text-red-600">{(item.estimated_ctr_impact * 100).toFixed(1)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <h3 className="text-lg font-semibold text-gray-900 mb-3">Top Competitors</h3>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Domain</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Shared Keywords</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Avg Position</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Threat Level</th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {report.module_3.competitors.map((comp, idx) => (
                        <tr key={idx}>
                          <td className="px-4 py-3 text-sm text-gray-900">{comp.domain}</td>
                          <td className="px-4 py-3 text-sm text-gray-900">{comp.keywords_shared}</td>
                          <td className="px-4 py-3 text-sm text-gray-900">{comp.avg_position.toFixed(1)}</td>
                          <td className="px-4 py-3 text-sm">
                            <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                              comp.threat_level === 'high' ? 'bg-red-100 text-red-800' :
                              comp.threat_level === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                              'bg-green-100 text-green-800'
                            }`}>
                              {comp.threat_level}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}

          {activeTab === 'content' && report.module_4 && (
            <div className="space-y-6">
              <div className="bg-white rounded-lg shadow-md p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-6">Content Intelligence</h2>

                <h3 className="text-lg font-semibold text-gray-900 mb-3">Cannibalization Clusters</h3>
                <div className="space-y-4 mb-6">
                  {report.module_4.cannibalization_clusters.map((cluster, idx) => (
                    <div key={idx} className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                      <div className="font-semibold text-gray-900 mb-2">{cluster.query_group}</div>
                      <div className="text-sm text-gray-600 space-y-1">
                        <div>Pages: {cluster.pages.join(', ')}</div>
                        <div>Shared Queries: {cluster.shared_queries}</div>
                        <div>Impressions Affected: {cluster.total_impressions_affected.toLocaleString()}</div>
                        <div className="font-medium text-yellow-800">
                          Recommendation: {cluster.recommendation}
                          {cluster.keep_page && ` (keep: ${cluster.keep_page})`}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>

                <h3 className="text-lg font-semibold text-gray-900 mb-3">Striking Distance Keywords</h3>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Query</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Position</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Impressions</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Click Gain</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Intent</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Page</th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {report.module_4.striking_distance.map((kw, idx) => (
                        <tr key={idx}>
                          <td className="px-4 py-3 text-sm text-gray-900">{kw.query}</td>
                          <td className="px-4 py-3 text-sm text-gray-900">{kw.current_position.toFixed(1)}</td>
                          <td className="px-4 py-3 text-sm text-gray-900">{kw.impressions.toLocaleString()}</td>
                          <td className="px-4 py-3 text-sm text-green-600 font-medium">+{kw.estimated_click_gain_if_top5}</td>
                          <td className="px-4 py-3 text-sm text-gray-600">{kw.intent}</td>
                          <td className="px-4 py-3 text-sm text-gray-600 max-w-xs truncate">{kw.landing_page}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}

          {activeTab === 'gameplan' && report.module_5 && (
            <div className="space-y-6">
              <div className="bg-white rounded-lg shadow-md p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">The Gameplan</h2>
                
                <div className="bg-gradient-to-r from-blue-50 to-purple-50 border border-blue-200 rounded-lg p-6 mb-6">
                  <h3 className="text-xl font-semibold text-gray-900 mb-3">Executive Summary</h3>
                  <p className="text-gray-700 leading-relaxed mb-4">{report.module_5.narrative}</p>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="bg-white rounded-lg p-4">
                      <div className="text-2xl font-bold text-green-600">
                        +{report.module_5.total_estimated_monthly_click_recovery.toLocaleString()}
                      </div>
                      <div className="text-sm text-gray-600">Monthly Clicks Recoverable</div>
                    </div>
                    <div className="bg-white rounded-lg p-4">
                      <div className="text-2xl font-bold text-blue-600">
                        +{report.module_5.total_estimated_monthly_click_growth.toLocaleString()}
                      </div>
                      <div className="text-sm text-gray-600">Monthly Clicks Growth Potential</div>
                    </div>
                  </div>
                </div>

                <div className="space-y-6">
                  <div>
                    <h3 className="text-lg font-semibold text-red-600 mb-3 flex items-center">
                      <span className="mr-2">🚨</span> Critical Fixes (Do This Week)
                    </h3>
                    <div className="space-y-3">
                      {report.module_5.critical.map((item, idx) => (
                        <div key={idx} className="bg-red-50 border border-red-200 rounded-lg p-4">
                          <div className="flex justify-between items-start mb-2">
                            <div className="font-medium text-gray-900">{item.action}</div>
                            <div className="flex items-center space-x-2">
                              <span className="text-sm px-2 py-1 bg-white rounded text-gray-700">
                                Impact: +{item.impact} clicks/mo
                              </span>
                              <span className={`text-sm px-2 py-1 rounded ${
                                item.effort === 'low' ? 'bg-green-100 text-green-800' :
                                item.effort === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                                'bg-red-100 text-red-800'
                              }`}>
                                {item.effort} effort
                              </span>
                            </div>
                          </div>
                          {item.page && <div className="text-sm text-gray-600">Page: {item.page}</div>}
                          {item.keyword && <div className="text-sm text-gray-600">Keyword: {item.keyword}</div>}
                        </div>
                      ))}
                    </div>
                  </div>

                  <div>
                    <h3 className="text-lg font-semibold text-yellow-600 mb-3 flex items-center">
                      <span className="mr-2">⚡</span> Quick Wins (Do This Month)
                    </h3>
                    <div className="space-y-3">
                      {report.module_5.quick_wins.map((item, idx) => (
                        <div key={idx} className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                          <div className="flex justify-between items-start mb-2">
                            <div className="font-medium text-gray-900">{item.action}</div>
                            <div className="flex items-center space-x-2">
                              <span className="text-sm px-2 py-1 bg-white rounded text-gray-700">
                                Impact: +{item.impact} clicks/mo
                              </span>
                              <span className={`text-sm px-2 py-1 rounded ${
                                item.effort === 'low' ? 'bg-green-100 text-green-800' :
                                item.effort === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                                'bg-red-100 text-red-800'
                              }`}>
                                {item.effort} effort
                              </span>
                            </div>
                          </div>
                          {item.page && <div className="text-sm text-gray-600">Page: {item.page}</div>}
                        </div>
                      ))}
                    </div>
                  </div>

                  <div>
                    <h3 className="text-lg font-semibold text-blue-600 mb-3 flex items-center">
                      <span className="mr-2">🎯</span> Strategic Plays (This Quarter)
                    </h3>
                    <div className="space-y-3">
                      {report.module_5.strategic.map((item, idx) => (
                        <div key={idx} className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                          <div className="flex justify-between items-start mb-2">
                            <div className="font-medium text-gray-900">{item.action}</div>
                            <div className="flex items-center space-x-2">
                              <span className="text-sm px-2 py-1 bg-white rounded text-gray-700">
                                Impact: +{item.impact} clicks/mo
                              </span>
                              <span className={`text-sm px-2 py-1 rounded ${
                                item.effort === 'low' ? 'bg-green-100 text-green-800' :
                                item.effort === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                                'bg-red-100 text-red-800'
                              }`}>
                                {item.effort} effort
                              </span>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>

                  <div>
                    <h3 className="text-lg font-semibold text-purple-600 mb-3 flex items-center">
                      <span className="mr-2">🏗️</span> Structural Improvements (Ongoing)
                    </h3>
                    <div className="space-y-3">
                      {report.module_5.structural.map((item, idx) => (
                        <div key={idx} className="bg-purple-50 border border-purple-200 rounded-lg p-4">
                          <div className="font-medium text-gray-900">{item.action}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Module Not Available Message */}
          {((activeTab === 'overview' && !report.module_1) ||
            (activeTab === 'pages' && !report.module_2) ||
            (activeTab === 'serp' && !report.module_3) ||
            (activeTab === 'content' && !report.module_4) ||
            (activeTab === 'gameplan' && !report.module_5)) && (
            <div className="bg-white rounded-lg shadow-md p-12 text-center">
              <div className="text-gray-400 mb-4">
                <svg className="w-16 h-16 mx-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
              </div>
              <h3 className="text-xl font-semibold text-gray-900 mb-2">Module Not Available</h3>
              <p className="text-gray-600">
                This module has not been generated yet or is still processing.
              </p>
            </div>
          )}
        </main>
      </div>
    </>
  );
}