import React, { useEffect, useState } from 'react';
import { useRouter } from 'next/router';
import Head from 'next/head';
import axios from 'axios';
import Module1TrafficOverview from '../../components/report/Module1TrafficOverview';

interface Report {
  id: string;
  site_url: string;
  created_at: string;
  date_range_start: string;
  date_range_end: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  data?: {
    module1?: any;
    module2?: any;
    module3?: any;
    module4?: any;
    module5?: any;
    module6?: any;
    module7?: any;
    module8?: any;
    module9?: any;
    module10?: any;
    module11?: any;
    module12?: any;
  };
  error?: string;
}

type TabId = 'module1' | 'module2' | 'module3' | 'module4' | 'module5' | 'module6' | 'module7' | 'module8' | 'module9' | 'module10' | 'module11' | 'module12';

const MODULES = [
  { id: 'module1' as TabId, name: 'Health & Trajectory', label: 'Traffic Overview' },
  { id: 'module2' as TabId, name: 'Page-Level Triage', label: 'Page Triage' },
  { id: 'module3' as TabId, name: 'SERP Landscape Analysis', label: 'SERP Analysis' },
  { id: 'module4' as TabId, name: 'Content Intelligence', label: 'Content Intelligence' },
  { id: 'module5' as TabId, name: 'The Gameplan', label: 'Action Plan' },
  { id: 'module6' as TabId, name: 'Algorithm Update Impact', label: 'Algorithm Impact' },
  { id: 'module7' as TabId, name: 'Query Intent Migration', label: 'Intent Migration' },
  { id: 'module8' as TabId, name: 'Internal Link Graph', label: 'Link Analysis' },
  { id: 'module9' as TabId, name: 'Seasonality Intelligence', label: 'Seasonality' },
  { id: 'module10' as TabId, name: 'Conversion Correlation', label: 'Conversions' },
  { id: 'module11' as TabId, name: 'Predictive Traffic Model', label: 'Predictions' },
  { id: 'module12' as TabId, name: 'Executive Summary', label: 'Executive Summary' }
];

const ReportPage: React.FC = () => {
  const router = useRouter();
  const { reportId } = router.query;
  const [report, setReport] = useState<Report | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<TabId>('module1');
  const [pollingInterval, setPollingInterval] = useState<NodeJS.Timeout | null>(null);

  useEffect(() => {
    if (!reportId || typeof reportId !== 'string') {
      return;
    }

    fetchReport(reportId);

    return () => {
      if (pollingInterval) {
        clearInterval(pollingInterval);
      }
    };
  }, [reportId]);

  const fetchReport = async (id: string) => {
    try {
      setLoading(true);
      const response = await axios.get(`/api/reports/${id}`);
      const reportData = response.data;
      setReport(reportData);
      setError(null);

      if (reportData.status === 'processing' || reportData.status === 'pending') {
        if (!pollingInterval) {
          const interval = setInterval(() => {
            pollReport(id);
          }, 5000);
          setPollingInterval(interval);
        }
      } else {
        if (pollingInterval) {
          clearInterval(pollingInterval);
          setPollingInterval(null);
        }
      }
    } catch (err: any) {
      if (err.response?.status === 404) {
        setError('Report not found. Please check the URL and try again.');
      } else if (err.response?.status === 500) {
        setError('Server error occurred while loading the report. Please try again later.');
      } else {
        setError('Failed to load report. Please try again.');
      }
      console.error('Error fetching report:', err);
    } finally {
      setLoading(false);
    }
  };

  const pollReport = async (id: string) => {
    try {
      const response = await axios.get(`/api/reports/${id}`);
      const reportData = response.data;
      setReport(reportData);

      if (reportData.status === 'completed' || reportData.status === 'failed') {
        if (pollingInterval) {
          clearInterval(pollingInterval);
          setPollingInterval(null);
        }
      }
    } catch (err) {
      console.error('Error polling report:', err);
    }
  };

  const formatDate = (dateString: string): string => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { 
      year: 'numeric', 
      month: 'long', 
      day: 'numeric' 
    });
  };

  const formatDateShort = (dateString: string): string => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { 
      year: 'numeric', 
      month: 'short', 
      day: 'numeric' 
    });
  };

  if (loading && !report) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading report...</p>
        </div>
      </div>
    );
  }

  if (error || !report) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="max-w-md w-full bg-white shadow-lg rounded-lg p-8">
          <div className="text-center">
            <svg className="mx-auto h-12 w-12 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
            </svg>
            <h2 className="mt-4 text-xl font-semibold text-gray-900">Error Loading Report</h2>
            <p className="mt-2 text-gray-600">{error || 'An unexpected error occurred'}</p>
            <button
              onClick={() => router.push('/dashboard')}
              className="mt-6 inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
            >
              Back to Dashboard
            </button>
          </div>
        </div>
      </div>
    );
  }

  if (report.status === 'failed') {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="max-w-md w-full bg-white shadow-lg rounded-lg p-8">
          <div className="text-center">
            <svg className="mx-auto h-12 w-12 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
            <h2 className="mt-4 text-xl font-semibold text-gray-900">Report Generation Failed</h2>
            <p className="mt-2 text-gray-600">{report.error || 'The report could not be generated. Please try again.'}</p>
            <div className="mt-6 space-x-3">
              <button
                onClick={() => router.push('/dashboard')}
                className="inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md shadow-sm text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
              >
                Back to Dashboard
              </button>
              <button
                onClick={() => window.location.reload()}
                className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
              >
                Retry
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (report.status === 'processing' || report.status === 'pending') {
    return (
      <div className="min-h-screen bg-gray-50">
        <Head>
          <title>Generating Report - Search Intelligence</title>
        </Head>
        <div className="max-w-4xl mx-auto py-12 px-4 sm:px-6 lg:px-8">
          <div className="bg-white shadow-lg rounded-lg p-8">
            <div className="text-center">
              <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-blue-600 mx-auto"></div>
              <h2 className="mt-6 text-2xl font-bold text-gray-900">Generating Your Report</h2>
              <p className="mt-2 text-gray-600">Site: {report.site_url}</p>
              <p className="mt-4 text-gray-500">
                This typically takes 2-5 minutes. We're analyzing your search performance data,
                running statistical models, and generating insights.
              </p>
              <div className="mt-8">
                <div className="w-full bg-gray-200 rounded-full h-2">
                  <div className="bg-blue-600 h-2 rounded-full animate-pulse" style={{ width: '60%' }}></div>
                </div>
                <p className="mt-2 text-sm text-gray-500">Processing data...</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <Head>
        <title>Search Intelligence Report - {report.site_url}</title>
        <meta name="description" content={`Comprehensive search intelligence report for ${report.site_url}`} />
      </Head>

      <div className="bg-white shadow-sm border-b border-gray-200 sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Search Intelligence Report</h1>
              <div className="mt-1 flex items-center space-x-4 text-sm text-gray-500">
                <span className="flex items-center">
                  <svg className="h-4 w-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9" />
                  </svg>
                  {report.site_url}
                </span>
                <span className="flex items-center">
                  <svg className="h-4 w-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
                  </svg>
                  {formatDateShort(report.date_range_start)} - {formatDateShort(report.date_range_end)}
                </span>
                <span className="flex items-center">
                  <svg className="h-4 w-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  Generated {formatDate(report.created_at)}
                </span>
              </div>
            </div>
            <button
              onClick={() => router.push('/dashboard')}
              className="inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md shadow-sm text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
            >
              <svg className="h-4 w-4 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
              </svg>
              Back to Dashboard
            </button>
          </div>
        </div>
      </div>

      <div className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <nav className="-mb-px flex space-x-8 overflow-x-auto" aria-label="Tabs">
            {MODULES.map((module) => (
              <button
                key={module.id}
                onClick={() => setActiveTab(module.id)}
                className={`
                  whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm transition-colors
                  ${activeTab === module.id
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  }
                `}
              >
                {module.label}
              </button>
            ))}
          </nav>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {activeTab === 'module1' && (
          <div>
            {report.data?.module1 ? (
              <Module1TrafficOverview data={report.data.module1} />
            ) : (
              <div className="bg-white shadow rounded-lg p-12">
                <div className="text-center">
                  <svg className="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                  </svg>
                  <h3 className="mt-2 text-lg font-medium text-gray-900">No data available</h3>
                  <p className="mt-1 text-sm text-gray-500">Traffic overview data is not yet available for this report.</p>
                </div>
              </div>
            )}
          </div>
        )}

        {activeTab === 'module2' && (
          <div className="bg-white shadow rounded-lg p-12">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-yellow-100 mb-4">
                <svg className="h-8 w-8 text-yellow-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6v6m0 0v6m0-6h6m-6 0H6" />
                </svg>
              </div>
              <h3 className="text-xl font-semibold text-gray-900">Page-Level Triage</h3>
              <p className="mt-2 text-gray-500">Coming Soon</p>
              <p className="mt-1 text-sm text-gray-400">
                Detailed analysis of individual page performance, decay patterns, and priority scoring.
              </p>
            </div>
          </div>
        )}

        {activeTab === 'module3' && (
          <div className="bg-white shadow rounded-lg p-12">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-yellow-100 mb-4">
                <svg className="h-8 w-8 text-yellow-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                </svg>
              </div>
              <h3 className="text-xl font-semibold text-gray-900">SERP Landscape Analysis</h3>
              <p className="mt-2 text-gray-500">Coming Soon</p>
              <p className="mt-1 text-sm text-gray-400">
                SERP feature displacement analysis, competitor mapping, and click share estimation.
              </p>
            </div>
          </div>
        )}

        {activeTab === 'module4' && (
          <div className="bg-white shadow rounded-lg p-12">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-yellow-100 mb-4">
                <svg className="h-8 w-8 text-yellow-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
              </div>
              <h3 className="text-xl font-semibold text-gray-900">Content Intelligence</h3>
              <p className="mt-2 text-gray-500">Coming Soon</p>
              <p className="mt-1 text-sm text-gray-400">
                Cannibalization detection, striking distance opportunities, and content age analysis.
              </p>
            </div>
          </div>
        )}

        {activeTab === 'module5' && (
          <div className="bg-white shadow rounded-lg p-12">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-yellow-100 mb-4">
                <svg className="h-8 w-8 text-yellow-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4" />
                </svg>
              </div>
              <h3 className="text-xl font-semibold text-gray-900">The Gameplan</h3>
              <p className="mt-2 text-gray-500">Coming Soon</p>
              <p className="mt-1 text-sm text-gray-400">
                Prioritized action plan with critical fixes, quick wins, and strategic initiatives.
              </p>
            </div>
          </div>
        )}

        {activeTab === 'module6' && (
          <div className="bg-white shadow rounded-lg p-12">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-yellow-100 mb-4">
                <svg className="h-8 w-8 text-yellow-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
              </div>
              <h3 className="text-xl font-semibold text-gray-900">Algorithm Update Impact Analysis</h3>
              <p className="mt-2 text-gray-500">Coming Soon</p>
              <p className="mt-1 text-sm text-gray-400">
                Correlation of traffic changes with known Google algorithm updates and vulnerability assessment.
              </p>
            </div>
          </div>
        )}

        {activeTab === 'module7' && (
          <div className="bg-white shadow rounded-lg p-12">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-yellow-100 mb-4">
                <svg className="h-8 w-8 text-yellow-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4" />
                </svg>
              </div>
              <h3 className="text-xl font-semibold text-gray-900">Query Intent Migration Tracking</h3>
              <p className="mt-2 text-gray-500">Coming Soon</p>
              <p className="mt-1 text-sm text-gray-400">
                Analysis of how search intent for your keywords has evolved over time.
              </p>
            </div>
          </div>
        )}

        {activeTab === 'module8' && (
          <div className="bg-white shadow rounded-lg p-12">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-yellow-100 mb-4">
                <svg className="h-8 w-8 text-yellow-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9" />
                </svg>
              </div>
              <h3 className="text-xl font-semibold text-gray-900">Internal Link Graph Analysis</h3>
              <p className="mt-2 text-gray-500">Coming Soon</p>
              <p className="mt-1 text-sm text-gray-400">
                PageRank-based internal link architecture analysis and improvement opportunities.
              </p>
            </div>
          </div>
        )}

        {activeTab === 'module9' && (
          <div className="bg-white shadow rounded-lg p-12">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-yellow-100 mb-4">
                <svg className="h-8 w-8 text-yellow-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 15a4 4 0 004 4h9a5 5 0 10-.1-9.999 5.002 5.002 0 10-9.78 2.096A4.001 4.001 0 003 15z" />
                </svg>
              </div>
              <h3 className="text-xl font-semibold text-gray-900">Seasonality Intelligence</h3>
              <p className="mt-2 text-gray-500">Coming Soon</p>
              <p className="mt-1 text-sm text-gray-400">
                Detection of seasonal patterns and content calendar recommendations.
              </p>
            </div>
          </div>
        )}

        {activeTab === 'module10' && (
          <div className="bg-white shadow rounded-lg p-12">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-yellow-100 mb-4">
                <svg className="h-8 w-8 text-yellow-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                </svg>
              </div>
              <h3 className="text-xl font-semibold text-gray-900">Conversion Correlation Analysis</h3>
              <p className="mt-2 text-gray-500">Coming Soon</p>
              <p className="mt-1 text-sm text-gray-400">
                Identification of high-conversion search patterns and revenue attribution.
              </p>
            </div>
          </div>
        )}

        {activeTab === 'module11' && (
          <div className="bg-white shadow rounded-lg p-12">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-yellow-100 mb-4">
                <svg className="h-8 w-8 text-yellow-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
                </svg>
              </div>
              <h3 className="text-xl font-semibold text-gray-900">Predictive Traffic Model</h3>
              <p className="mt-2 text-gray-500">Coming Soon</p>
              <p className="mt-1 text-sm text-gray-400">
                ML-based traffic forecasting and scenario modeling for planned improvements.
              </p>
            </div>
          </div>
        )}

        {activeTab === 'module12' && (
          <div className="bg-white shadow rounded-lg p-12">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-yellow-100 mb-4">
                <svg className="h-8 w-8 text-yellow-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
              </div>
              <h3 className="text-xl font-semibold text-gray-900">Executive Summary</h3>
              <p className="mt-2 text-gray-500">Coming Soon</p>
              <p className="mt-1 text-sm text-gray-400">
                High-level overview with key metrics, top findings, and strategic recommendations.
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;