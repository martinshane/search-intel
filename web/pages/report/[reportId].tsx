import { useRouter } from 'next/router';
import React, { useEffect, useState } from 'react';
import Head from 'next/head';
import Link from 'next/link';
import Module1TrafficOverview from '../../components/modules/Module1TrafficOverview';

interface Report {
  id: string;
  site_url: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  created_at: string;
  updated_at: string;
  progress?: number;
  error?: string;
}

interface ReportData {
  health_trajectory?: any;
  page_triage?: any;
  serp_landscape?: any;
  content_intelligence?: any;
  gameplan?: any;
  algorithm_impacts?: any;
  [key: string]: any;
}

export default function ReportDetailPage() {
  const router = useRouter();
  const { reportId } = router.query;

  const [report, setReport] = useState<Report | null>(null);
  const [reportData, setReportData] = useState<ReportData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pollingInterval, setPollingInterval] = useState<NodeJS.Timeout | null>(null);

  // Fetch report metadata and data
  const fetchReport = async (id: string) => {
    try {
      const response = await fetch(`/api/reports/${id}`);
      
      if (!response.ok) {
        if (response.status === 404) {
          throw new Error('Report not found');
        }
        throw new Error('Failed to load report');
      }

      const data = await response.json();
      setReport(data.report);
      setReportData(data.data || null);
      setError(null);

      // If report is still processing, continue polling
      if (data.report.status === 'processing' || data.report.status === 'pending') {
        if (!pollingInterval) {
          const interval = setInterval(() => {
            fetchReport(id);
          }, 5000); // Poll every 5 seconds
          setPollingInterval(interval);
        }
      } else {
        // Report is completed or failed, stop polling
        if (pollingInterval) {
          clearInterval(pollingInterval);
          setPollingInterval(null);
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      setReport(null);
      setReportData(null);
      
      if (pollingInterval) {
        clearInterval(pollingInterval);
        setPollingInterval(null);
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (reportId && typeof reportId === 'string') {
      setLoading(true);
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
            <p className="text-gray-600">Please wait while we fetch your report data...</p>
          </div>
        </div>
      </>
    );
  }

  // Error state
  if (error || !report) {
    return (
      <>
        <Head>
          <title>Error | Search Intelligence Report</title>
        </Head>
        <div className="min-h-screen bg-gray-50 flex items-center justify-center px-4">
          <div className="max-w-md w-full bg-white shadow-lg rounded-lg p-8 text-center">
            <div className="w-16 h-16 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
              <svg className="w-8 h-8 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </div>
            <h2 className="text-2xl font-bold text-gray-900 mb-2">Error Loading Report</h2>
            <p className="text-gray-600 mb-6">{error || 'Report not found'}</p>
            <Link
              href="/dashboard"
              className="inline-block bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors font-medium"
            >
              Back to Dashboard
            </Link>
          </div>
        </div>
      </>
    );
  }

  // Processing state
  if (report.status === 'pending' || report.status === 'processing') {
    return (
      <>
        <Head>
          <title>Generating Report... | Search Intelligence Report</title>
        </Head>
        <div className="min-h-screen bg-gray-50">
          <div className="max-w-4xl mx-auto px-4 py-16">
            <div className="bg-white shadow-lg rounded-lg p-8">
              <div className="text-center mb-8">
                <div className="inline-block animate-spin rounded-full h-16 w-16 border-b-2 border-blue-600 mb-4"></div>
                <h1 className="text-3xl font-bold text-gray-900 mb-2">
                  {report.status === 'pending' ? 'Report Queued' : 'Generating Report'}
                </h1>
                <p className="text-xl text-gray-600 mb-4">{report.site_url}</p>
                <p className="text-gray-500">
                  {report.status === 'pending' 
                    ? 'Your report is in the queue and will begin processing shortly...'
                    : 'This typically takes 2-5 minutes. Please don\'t close this page.'}
                </p>
              </div>

              {report.progress !== undefined && (
                <div className="mb-6">
                  <div className="flex justify-between items-center mb-2">
                    <span className="text-sm font-medium text-gray-700">Progress</span>
                    <span className="text-sm font-medium text-gray-700">{Math.round(report.progress)}%</span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-3">
                    <div
                      className="bg-blue-600 h-3 rounded-full transition-all duration-500"
                      style={{ width: `${report.progress}%` }}
                    ></div>
                  </div>
                </div>
              )}

              <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                <h3 className="font-semibold text-blue-900 mb-2">What we're analyzing:</h3>
                <ul className="space-y-1 text-sm text-blue-800">
                  <li>• Pulling 16 months of Search Console data</li>
                  <li>• Analyzing Google Analytics 4 metrics</li>
                  <li>• Fetching live SERP data</li>
                  <li>• Running statistical analysis</li>
                  <li>• Detecting trends and anomalies</li>
                  <li>• Identifying opportunities</li>
                  <li>• Generating actionable insights</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      </>
    );
  }

  // Failed state
  if (report.status === 'failed') {
    return (
      <>
        <Head>
          <title>Report Failed | Search Intelligence Report</title>
        </Head>
        <div className="min-h-screen bg-gray-50 flex items-center justify-center px-4">
          <div className="max-w-md w-full bg-white shadow-lg rounded-lg p-8">
            <div className="w-16 h-16 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
              <svg className="w-8 h-8 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              </svg>
            </div>
            <h2 className="text-2xl font-bold text-gray-900 mb-2 text-center">Report Generation Failed</h2>
            <p className="text-gray-600 mb-2 text-center">{report.site_url}</p>
            {report.error && (
              <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-6">
                <p className="text-sm text-red-800">{report.error}</p>
              </div>
            )}
            <div className="flex gap-3">
              <Link
                href="/dashboard"
                className="flex-1 text-center bg-gray-200 text-gray-800 px-4 py-3 rounded-lg hover:bg-gray-300 transition-colors font-medium"
              >
                Back to Dashboard
              </Link>
              <button
                onClick={() => {
                  // TODO: Implement retry logic
                  alert('Retry functionality coming soon');
                }}
                className="flex-1 bg-blue-600 text-white px-4 py-3 rounded-lg hover:bg-blue-700 transition-colors font-medium"
              >
                Retry
              </button>
            </div>
          </div>
        </div>
      </>
    );
  }

  // Completed state - render the report
  return (
    <>
      <Head>
        <title>{report.site_url} - Search Intelligence Report</title>
        <meta name="description" content={`Comprehensive search intelligence analysis for ${report.site_url}`} />
      </Head>

      <div className="min-h-screen bg-gray-50">
        {/* Header */}
        <div className="bg-white border-b border-gray-200">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
            <div className="flex items-center justify-between">
              <div>
                <Link
                  href="/dashboard"
                  className="text-sm text-blue-600 hover:text-blue-800 mb-2 inline-flex items-center"
                >
                  <svg className="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
                  </svg>
                  Back to Dashboard
                </Link>
                <h1 className="text-3xl font-bold text-gray-900">{report.site_url}</h1>
                <p className="text-sm text-gray-500 mt-1">
                  Generated on {new Date(report.created_at).toLocaleDateString('en-US', {
                    year: 'numeric',
                    month: 'long',
                    day: 'numeric',
                    hour: '2-digit',
                    minute: '2-digit'
                  })}
                </p>
              </div>
              <div className="flex gap-3">
                <button
                  onClick={() => window.print()}
                  className="bg-gray-200 text-gray-800 px-4 py-2 rounded-lg hover:bg-gray-300 transition-colors font-medium inline-flex items-center"
                >
                  <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 17h2a2 2 0 002-2v-4a2 2 0 00-2-2H5a2 2 0 00-2 2v4a2 2 0 002 2h2m2 4h6a2 2 0 002-2v-4a2 2 0 00-2-2H9a2 2 0 00-2 2v4a2 2 0 002 2zm8-12V5a2 2 0 00-2-2H9a2 2 0 00-2 2v4h10z" />
                  </svg>
                  Print
                </button>
                <button
                  onClick={() => {
                    // TODO: Implement PDF export
                    alert('PDF export coming soon');
                  }}
                  className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors font-medium inline-flex items-center"
                >
                  <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                  </svg>
                  Export PDF
                </button>
              </div>
            </div>
          </div>
        </div>

        {/* Report Content */}
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="space-y-8">
            {/* Module 1: Traffic Overview & Health */}
            {reportData?.health_trajectory && (
              <Module1TrafficOverview data={reportData.health_trajectory} />
            )}

            {/* Module 2: Page-Level Triage - Placeholder */}
            <div className="bg-white rounded-lg shadow-lg p-8">
              <h2 className="text-2xl font-bold text-gray-900 mb-4">
                📊 Page-Level Triage
              </h2>
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 text-center">
                <p className="text-blue-800 font-medium mb-2">Module Coming Soon</p>
                <p className="text-sm text-blue-600">
                  This module will analyze individual page performance, identify decay patterns, 
                  detect CTR anomalies, and prioritize pages for optimization.
                </p>
              </div>
            </div>

            {/* Module 3: SERP Landscape - Placeholder */}
            <div className="bg-white rounded-lg shadow-lg p-8">
              <h2 className="text-2xl font-bold text-gray-900 mb-4">
                🔍 SERP Landscape Analysis
              </h2>
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 text-center">
                <p className="text-blue-800 font-medium mb-2">Module Coming Soon</p>
                <p className="text-sm text-blue-600">
                  This module will analyze SERP features, competitor positioning, 
                  intent classification, and click share estimation.
                </p>
              </div>
            </div>

            {/* Module 4: Content Intelligence - Placeholder */}
            <div className="bg-white rounded-lg shadow-lg p-8">
              <h2 className="text-2xl font-bold text-gray-900 mb-4">
                📝 Content Intelligence
              </h2>
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 text-center">
                <p className="text-blue-800 font-medium mb-2">Module Coming Soon</p>
                <p className="text-sm text-blue-600">
                  This module will detect cannibalization, identify striking distance opportunities, 
                  flag thin content, and create an update priority matrix.
                </p>
              </div>
            </div>

            {/* Module 5: The Gameplan - Placeholder */}
            <div className="bg-white rounded-lg shadow-lg p-8">
              <h2 className="text-2xl font-bold text-gray-900 mb-4">
                🎯 The Gameplan
              </h2>
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 text-center">
                <p className="text-blue-800 font-medium mb-2">Module Coming Soon</p>
                <p className="text-sm text-blue-600">
                  This module will synthesize all insights into prioritized action items 
                  with estimated impact and effort levels.
                </p>
              </div>
            </div>

            {/* Module 6: Algorithm Impact - Placeholder */}
            <div className="bg-white rounded-lg shadow-lg p-8">
              <h2 className="text-2xl font-bold text-gray-900 mb-4">
                🔄 Algorithm Update Impact
              </h2>
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 text-center">
                <p className="text-blue-800 font-medium mb-2">Module Coming Soon</p>
                <p className="text-sm text-blue-600">
                  This module will correlate traffic changes with Google algorithm updates 
                  and assess historical vulnerability.
                </p>
              </div>
            </div>

            {/* Additional Modules - Placeholders */}
            <div className="bg-white rounded-lg shadow-lg p-8">
              <h2 className="text-2xl font-bold text-gray-900 mb-4">
                📈 Additional Analysis Modules
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h3 className="font-semibold text-gray-900 mb-2">Query Intent Migration</h3>
                  <p className="text-sm text-gray-600">Track how search intent evolves over time</p>
                </div>
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h3 className="font-semibold text-gray-900 mb-2">Technical SEO Audit</h3>
                  <p className="text-sm text-gray-600">Indexing, crawlability, and site health</p>
                </div>
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h3 className="font-semibold text-gray-900 mb-2">Internal Link Analysis</h3>
                  <p className="text-sm text-gray-600">PageRank flow and link equity optimization</p>
                </div>
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h3 className="font-semibold text-gray-900 mb-2">Competitive Intelligence</h3>
                  <p className="text-sm text-gray-600">Deep competitor analysis and gap identification</p>
                </div>
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h3 className="font-semibold text-gray-900 mb-2">Seasonality Modeling</h3>
                  <p className="text-sm text-gray-600">Predictive models for traffic patterns</p>
                </div>
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h3 className="font-semibold text-gray-900 mb-2">Content Refresh ROI</h3>
                  <p className="text-sm text-gray-600">Quantified impact of content updates</p>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="bg-white border-t border-gray-200 mt-12">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
            <p className="text-sm text-gray-500 text-center">
              Search Intelligence Report • Generated by combining GSC, GA4, and SERP data with advanced statistical analysis
            </p>
          </div>
        </div>
      </div>
    </>
  );
}