import React, { useEffect, useState } from 'react';
import { useRouter } from 'next/router';
import Head from 'next/head';
import { supabase } from '../../lib/supabase';
import Module1Performance from '../../components/Module1Performance';

interface ReportModules {
  module_1_search_performance?: {
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
      "30d": { clicks: number; ci_low: number; ci_high: number };
      "60d": { clicks: number; ci_low: number; ci_high: number };
      "90d": { clicks: number; ci_low: number; ci_high: number };
    };
  };
  module_2_page_triage?: any;
  module_3_serp_landscape?: any;
  module_4_content_intelligence?: any;
  module_5_gameplan?: any;
  module_6_algorithm_impacts?: any;
}

interface Report {
  id: string;
  user_id: string;
  site_url: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  created_at: string;
  completed_at?: string;
  error_message?: string;
  modules: ReportModules;
  metadata?: {
    gsc_property?: string;
    ga4_property?: string;
    date_range_start?: string;
    date_range_end?: string;
  };
}

const ReportDetailPage: React.FC = () => {
  const router = useRouter();
  const { reportId } = router.query;
  
  const [report, setReport] = useState<Report | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pollingInterval, setPollingInterval] = useState<NodeJS.Timeout | null>(null);

  // Fetch report data
  const fetchReport = async (id: string) => {
    try {
      const { data, error } = await supabase
        .from('reports')
        .select('*')
        .eq('id', id)
        .single();

      if (error) throw error;

      if (!data) {
        setError('Report not found');
        setLoading(false);
        return;
      }

      setReport(data);
      setError(null);

      // If report is still processing, continue polling
      if (data.status === 'pending' || data.status === 'processing') {
        if (!pollingInterval) {
          const interval = setInterval(() => fetchReport(id), 5000);
          setPollingInterval(interval);
        }
      } else {
        // Stop polling if report is completed or failed
        if (pollingInterval) {
          clearInterval(pollingInterval);
          setPollingInterval(null);
        }
      }

      setLoading(false);
    } catch (err) {
      console.error('Error fetching report:', err);
      setError(err instanceof Error ? err.message : 'Failed to load report');
      setLoading(false);
    }
  };

  useEffect(() => {
    if (reportId && typeof reportId === 'string') {
      fetchReport(reportId);
    }

    // Cleanup polling on unmount
    return () => {
      if (pollingInterval) {
        clearInterval(pollingInterval);
      }
    };
  }, [reportId]);

  // Loading state
  if (loading && !report) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading report...</p>
        </div>
      </div>
    );
  }

  // Error state
  if (error && !report) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="max-w-md w-full bg-white rounded-lg shadow-lg p-8">
          <div className="text-red-600 mb-4">
            <svg className="h-12 w-12 mx-auto" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h2 className="text-2xl font-bold text-gray-900 text-center mb-2">Error Loading Report</h2>
          <p className="text-gray-600 text-center mb-6">{error}</p>
          <button
            onClick={() => router.push('/dashboard')}
            className="w-full bg-blue-600 text-white py-2 px-4 rounded-md hover:bg-blue-700 transition-colors"
          >
            Back to Dashboard
          </button>
        </div>
      </div>
    );
  }

  if (!report) {
    return null;
  }

  // Report processing state
  if (report.status === 'pending' || report.status === 'processing') {
    return (
      <div className="min-h-screen bg-gray-50">
        <Head>
          <title>Generating Report | Search Intelligence</title>
        </Head>
        <div className="max-w-4xl mx-auto px-4 py-12">
          <div className="bg-white rounded-lg shadow-lg p-8">
            <div className="text-center">
              <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-blue-600 mx-auto mb-6"></div>
              <h1 className="text-3xl font-bold text-gray-900 mb-4">
                {report.status === 'pending' ? 'Report Queued' : 'Generating Your Report'}
              </h1>
              <p className="text-xl text-gray-600 mb-2">{report.site_url}</p>
              <p className="text-gray-500 mb-8">
                This typically takes 2-5 minutes. You can safely close this page and return later.
              </p>
              <div className="bg-blue-50 border border-blue-200 rounded-md p-4 mb-6">
                <p className="text-sm text-blue-800">
                  We're analyzing your site's search performance across multiple data sources including
                  Google Search Console, Google Analytics, and live SERP data.
                </p>
              </div>
              <button
                onClick={() => router.push('/dashboard')}
                className="text-blue-600 hover:text-blue-700 font-medium"
              >
                ← Back to Dashboard
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // Report failed state
  if (report.status === 'failed') {
    return (
      <div className="min-h-screen bg-gray-50">
        <Head>
          <title>Report Failed | Search Intelligence</title>
        </Head>
        <div className="max-w-4xl mx-auto px-4 py-12">
          <div className="bg-white rounded-lg shadow-lg p-8">
            <div className="text-center">
              <div className="text-red-600 mb-4">
                <svg className="h-16 w-16 mx-auto" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
              </div>
              <h1 className="text-3xl font-bold text-gray-900 mb-4">Report Generation Failed</h1>
              <p className="text-xl text-gray-600 mb-2">{report.site_url}</p>
              {report.error_message && (
                <div className="bg-red-50 border border-red-200 rounded-md p-4 mb-6 text-left">
                  <p className="text-sm font-medium text-red-800 mb-1">Error Details:</p>
                  <p className="text-sm text-red-700">{report.error_message}</p>
                </div>
              )}
              <div className="space-y-3">
                <button
                  onClick={() => {
                    // Trigger report regeneration
                    fetch(`/api/reports/${report.id}/regenerate`, { method: 'POST' })
                      .then(() => {
                        setReport({ ...report, status: 'pending' });
                        fetchReport(report.id);
                      })
                      .catch(err => setError('Failed to regenerate report'));
                  }}
                  className="w-full bg-blue-600 text-white py-2 px-4 rounded-md hover:bg-blue-700 transition-colors"
                >
                  Try Again
                </button>
                <button
                  onClick={() => router.push('/dashboard')}
                  className="w-full bg-gray-200 text-gray-700 py-2 px-4 rounded-md hover:bg-gray-300 transition-colors"
                >
                  Back to Dashboard
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // Report completed - render full report
  const module1Data = report.modules?.module_1_search_performance;

  return (
    <div className="min-h-screen bg-gray-50">
      <Head>
        <title>Search Intelligence Report - {report.site_url}</title>
        <meta name="description" content={`Comprehensive search intelligence analysis for ${report.site_url}`} />
      </Head>

      {/* Header */}
      <div className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <button
                onClick={() => router.push('/dashboard')}
                className="text-blue-600 hover:text-blue-700 font-medium mb-2 flex items-center"
              >
                <svg className="w-4 h-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
                </svg>
                Dashboard
              </button>
              <h1 className="text-3xl font-bold text-gray-900">{report.site_url}</h1>
              <p className="text-sm text-gray-500 mt-1">
                Generated {new Date(report.created_at).toLocaleDateString('en-US', {
                  year: 'numeric',
                  month: 'long',
                  day: 'numeric',
                  hour: '2-digit',
                  minute: '2-digit'
                })}
              </p>
            </div>
            <div className="flex space-x-3">
              <button className="bg-white border border-gray-300 text-gray-700 py-2 px-4 rounded-md hover:bg-gray-50 transition-colors">
                Export PDF
              </button>
              <button className="bg-blue-600 text-white py-2 px-4 rounded-md hover:bg-blue-700 transition-colors">
                Share Report
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Report Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Executive Summary */}
        <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
          <h2 className="text-2xl font-bold text-gray-900 mb-4">Executive Summary</h2>
          <p className="text-gray-600 mb-4">
            This comprehensive Search Intelligence Report analyzes your site's search performance across
            multiple dimensions including traffic trends, page-level health, SERP landscape, content
            opportunities, and actionable recommendations.
          </p>
          {report.metadata && (
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-6">
              {report.metadata.date_range_start && (
                <div className="bg-gray-50 rounded-md p-4">
                  <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">Date Range</p>
                  <p className="text-sm font-medium text-gray-900">
                    {new Date(report.metadata.date_range_start).toLocaleDateString()} - 
                    {report.metadata.date_range_end && ` ${new Date(report.metadata.date_range_end).toLocaleDateString()}`}
                  </p>
                </div>
              )}
              {report.metadata.gsc_property && (
                <div className="bg-gray-50 rounded-md p-4">
                  <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">GSC Property</p>
                  <p className="text-sm font-medium text-gray-900 truncate">{report.metadata.gsc_property}</p>
                </div>
              )}
              {report.metadata.ga4_property && (
                <div className="bg-gray-50 rounded-md p-4">
                  <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">GA4 Property</p>
                  <p className="text-sm font-medium text-gray-900 truncate">{report.metadata.ga4_property}</p>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Module 1: Health & Trajectory */}
        {module1Data ? (
          <Module1Performance data={module1Data} />
        ) : (
          <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">1. Health & Trajectory Analysis</h2>
            <div className="bg-yellow-50 border border-yellow-200 rounded-md p-4">
              <div className="flex">
                <div className="flex-shrink-0">
                  <svg className="h-5 w-5 text-yellow-400" viewBox="0 0 20 20" fill="currentColor">
                    <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                  </svg>
                </div>
                <div className="ml-3">
                  <h3 className="text-sm font-medium text-yellow-800">Module data not yet available</h3>
                  <p className="mt-1 text-sm text-yellow-700">
                    The Health & Trajectory analysis is still being generated. This module analyzes
                    traffic trends, seasonality patterns, change points, and forecasts future performance.
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Module 2: Page-Level Triage */}
        {report.modules?.module_2_page_triage ? (
          <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">2. Page-Level Triage</h2>
            <p className="text-gray-600">Module 2 component will be rendered here</p>
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">2. Page-Level Triage</h2>
            <div className="bg-gray-50 border border-gray-200 rounded-md p-4">
              <p className="text-sm text-gray-600">
                Page-level performance analysis is being generated. This will identify pages that are
                growing, stable, decaying, or in critical condition.
              </p>
            </div>
          </div>
        )}

        {/* Module 3: SERP Landscape Analysis */}
        {report.modules?.module_3_serp_landscape ? (
          <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">3. SERP Landscape Analysis</h2>
            <p className="text-gray-600">Module 3 component will be rendered here</p>
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">3. SERP Landscape Analysis</h2>
            <div className="bg-gray-50 border border-gray-200 rounded-md p-4">
              <p className="text-sm text-gray-600">
                SERP analysis is being generated. This will analyze SERP features, competitor positions,
                and click share estimates.
              </p>
            </div>
          </div>
        )}

        {/* Module 4: Content Intelligence */}
        {report.modules?.module_4_content_intelligence ? (
          <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">4. Content Intelligence</h2>
            <p className="text-gray-600">Module 4 component will be rendered here</p>
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">4. Content Intelligence</h2>
            <div className="bg-gray-50 border border-gray-200 rounded-md p-4">
              <p className="text-sm text-gray-600">
                Content analysis is being generated. This will identify cannibalization issues, striking
                distance opportunities, and content gaps.
              </p>
            </div>
          </div>
        )}

        {/* Module 5: The Gameplan */}
        {report.modules?.module_5_gameplan ? (
          <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">5. The Gameplan</h2>
            <p className="text-gray-600">Module 5 component will be rendered here</p>
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">5. The Gameplan</h2>
            <div className="bg-gray-50 border border-gray-200 rounded-md p-4">
              <p className="text-sm text-gray-600">
                Your prioritized action plan is being generated. This will synthesize all findings into
                critical fixes, quick wins, and strategic plays.
              </p>
            </div>
          </div>
        )}

        {/* Module 6: Algorithm Update Impact */}
        {report.modules?.module_6_algorithm_impacts ? (
          <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">6. Algorithm Update Impact Analysis</h2>
            <p className="text-gray-600">Module 6 component will be rendered here</p>
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">6. Algorithm Update Impact Analysis</h2>
            <div className="bg-gray-50 border border-gray-200 rounded-md p-4">
              <p className="text-sm text-gray-600">
                Algorithm impact analysis is being generated. This will identify how Google updates have
                affected your site and your vulnerability to future updates.
              </p>
            </div>
          </div>
        )}

        {/* Footer */}
        <div className="bg-white rounded-lg shadow-lg p-8 mt-8">
          <div className="text-center">
            <h3 className="text-xl font-bold text-gray-900 mb-2">Need Help Implementing These Recommendations?</h3>
            <p className="text-gray-600 mb-6">
              Our team of search experts can help you execute this gameplan and recover lost traffic.
            </p>
            <button className="bg-blue-600 text-white py-3 px-8 rounded-md hover:bg-blue-700 transition-colors font-medium">
              Schedule a Consultation
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ReportDetailPage;