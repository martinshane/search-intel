import { useRouter } from 'next/router';
import { useEffect, useState } from 'react';
import Head from 'next/head';
import Link from 'next/link';
import ModuleOne from '../../components/ModuleOne';

interface Report {
  id: string;
  domain: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  created_at: string;
  updated_at: string;
  progress?: number;
  error_message?: string;
}

interface ModuleOneData {
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
    '30d': { clicks: number; ci_low: number; ci_high: number };
    '60d': { clicks: number; ci_low: number; ci_high: number };
    '90d': { clicks: number; ci_low: number; ci_high: number };
  };
}

const ReportDetail = () => {
  const router = useRouter();
  const { reportId } = router.query;

  const [report, setReport] = useState<Report | null>(null);
  const [moduleOneData, setModuleOneData] = useState<ModuleOneData | null>(null);
  const [isLoadingReport, setIsLoadingReport] = useState(true);
  const [isLoadingModuleOne, setIsLoadingModuleOne] = useState(false);
  const [reportError, setReportError] = useState<string | null>(null);
  const [moduleOneError, setModuleOneError] = useState<string | null>(null);
  const [pollInterval, setPollInterval] = useState<NodeJS.Timeout | null>(null);

  // Fetch report details
  useEffect(() => {
    if (!reportId) return;

    const fetchReport = async () => {
      try {
        setIsLoadingReport(true);
        setReportError(null);

        const response = await fetch(`/api/reports/${reportId}`, {
          headers: {
            'Content-Type': 'application/json',
          },
          credentials: 'include',
        });

        if (!response.ok) {
          if (response.status === 404) {
            throw new Error('Report not found');
          }
          if (response.status === 401) {
            router.push('/login');
            return;
          }
          throw new Error(`Failed to fetch report: ${response.statusText}`);
        }

        const data = await response.json();
        setReport(data);

        // If report is completed, fetch module one data
        if (data.status === 'completed') {
          fetchModuleOneData();
        }

        // Set up polling if report is still processing
        if (data.status === 'processing' || data.status === 'pending') {
          if (!pollInterval) {
            const interval = setInterval(() => {
              fetchReport();
            }, 5000); // Poll every 5 seconds
            setPollInterval(interval);
          }
        } else {
          // Clear polling if report is completed or failed
          if (pollInterval) {
            clearInterval(pollInterval);
            setPollInterval(null);
          }
        }
      } catch (error) {
        console.error('Error fetching report:', error);
        setReportError(error instanceof Error ? error.message : 'An error occurred');
        if (pollInterval) {
          clearInterval(pollInterval);
          setPollInterval(null);
        }
      } finally {
        setIsLoadingReport(false);
      }
    };

    fetchReport();

    // Cleanup polling on unmount
    return () => {
      if (pollInterval) {
        clearInterval(pollInterval);
      }
    };
  }, [reportId, router]);

  // Fetch Module One data
  const fetchModuleOneData = async () => {
    if (!reportId) return;

    try {
      setIsLoadingModuleOne(true);
      setModuleOneError(null);

      const response = await fetch(`/api/reports/${reportId}/modules/1`, {
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
      });

      if (!response.ok) {
        if (response.status === 404) {
          throw new Error('Module data not found');
        }
        if (response.status === 401) {
          router.push('/login');
          return;
        }
        throw new Error(`Failed to fetch module data: ${response.statusText}`);
      }

      const data = await response.json();
      setModuleOneData(data);
    } catch (error) {
      console.error('Error fetching module one data:', error);
      setModuleOneError(error instanceof Error ? error.message : 'Failed to load module data');
    } finally {
      setIsLoadingModuleOne(false);
    }
  };

  if (isLoadingReport && !report) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading report...</p>
        </div>
      </div>
    );
  }

  if (reportError) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="max-w-md w-full bg-white shadow-lg rounded-lg p-8">
          <div className="text-center">
            <div className="text-red-600 text-5xl mb-4">⚠️</div>
            <h2 className="text-2xl font-bold text-gray-900 mb-2">Error</h2>
            <p className="text-gray-600 mb-6">{reportError}</p>
            <Link
              href="/dashboard"
              className="inline-block bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700 transition-colors"
            >
              Back to Dashboard
            </Link>
          </div>
        </div>
      </div>
    );
  }

  if (!report) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="max-w-md w-full bg-white shadow-lg rounded-lg p-8">
          <div className="text-center">
            <h2 className="text-2xl font-bold text-gray-900 mb-2">Report Not Found</h2>
            <p className="text-gray-600 mb-6">The requested report could not be found.</p>
            <Link
              href="/dashboard"
              className="inline-block bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700 transition-colors"
            >
              Back to Dashboard
            </Link>
          </div>
        </div>
      </div>
    );
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return 'bg-green-100 text-green-800';
      case 'processing':
        return 'bg-blue-100 text-blue-800';
      case 'pending':
        return 'bg-yellow-100 text-yellow-800';
      case 'failed':
        return 'bg-red-100 text-red-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  const getStatusText = (status: string) => {
    switch (status) {
      case 'completed':
        return 'Completed';
      case 'processing':
        return 'Processing';
      case 'pending':
        return 'Pending';
      case 'failed':
        return 'Failed';
      default:
        return status;
    }
  };

  return (
    <>
      <Head>
        <title>{`Report for ${report.domain} | Search Intelligence Report`}</title>
        <meta name="description" content={`Search Intelligence Report for ${report.domain}`} />
      </Head>

      <div className="min-h-screen bg-gray-50">
        {/* Header */}
        <div className="bg-white shadow">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-4">
                <Link
                  href="/dashboard"
                  className="text-gray-500 hover:text-gray-700 transition-colors"
                >
                  ← Back to Dashboard
                </Link>
                <div className="h-6 w-px bg-gray-300"></div>
                <h1 className="text-2xl font-bold text-gray-900">{report.domain}</h1>
              </div>
              <div className="flex items-center space-x-4">
                <span className={`px-3 py-1 rounded-full text-sm font-medium ${getStatusColor(report.status)}`}>
                  {getStatusText(report.status)}
                </span>
              </div>
            </div>
            <div className="mt-2 text-sm text-gray-500">
              Created {new Date(report.created_at).toLocaleDateString()} at{' '}
              {new Date(report.created_at).toLocaleTimeString()}
            </div>
          </div>
        </div>

        {/* Content */}
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          {/* Processing Status */}
          {(report.status === 'processing' || report.status === 'pending') && (
            <div className="bg-white rounded-lg shadow p-6 mb-6">
              <div className="flex items-center space-x-4">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                <div className="flex-1">
                  <h3 className="text-lg font-semibold text-gray-900">
                    {report.status === 'pending' ? 'Report Queued' : 'Generating Report'}
                  </h3>
                  <p className="text-sm text-gray-600 mt-1">
                    {report.status === 'pending'
                      ? 'Your report is queued and will begin processing shortly...'
                      : 'This may take 2-5 minutes. You can leave this page and check back later.'}
                  </p>
                  {report.progress !== undefined && (
                    <div className="mt-4">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-sm font-medium text-gray-700">Progress</span>
                        <span className="text-sm font-medium text-gray-700">{report.progress}%</span>
                      </div>
                      <div className="w-full bg-gray-200 rounded-full h-2">
                        <div
                          className="bg-blue-600 h-2 rounded-full transition-all duration-300"
                          style={{ width: `${report.progress}%` }}
                        ></div>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* Failed Status */}
          {report.status === 'failed' && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-6 mb-6">
              <div className="flex items-start space-x-3">
                <div className="text-red-600 text-2xl">⚠️</div>
                <div className="flex-1">
                  <h3 className="text-lg font-semibold text-red-900">Report Generation Failed</h3>
                  <p className="text-sm text-red-700 mt-1">
                    {report.error_message || 'An error occurred while generating the report.'}
                  </p>
                  <button
                    onClick={() => {
                      // TODO: Implement retry logic
                      alert('Retry functionality coming soon');
                    }}
                    className="mt-4 bg-red-600 text-white px-4 py-2 rounded-lg hover:bg-red-700 transition-colors text-sm"
                  >
                    Retry Report Generation
                  </button>
                </div>
              </div>
            </div>
          )}

          {/* Report Content - Only show when completed */}
          {report.status === 'completed' && (
            <div className="space-y-6">
              {/* Module One */}
              {isLoadingModuleOne && !moduleOneData && (
                <div className="bg-white rounded-lg shadow p-8">
                  <div className="text-center">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto mb-4"></div>
                    <p className="text-gray-600">Loading module data...</p>
                  </div>
                </div>
              )}

              {moduleOneError && !moduleOneData && (
                <div className="bg-red-50 border border-red-200 rounded-lg p-6">
                  <div className="flex items-start space-x-3">
                    <div className="text-red-600 text-xl">⚠️</div>
                    <div className="flex-1">
                      <h3 className="text-sm font-semibold text-red-900">Failed to Load Module Data</h3>
                      <p className="text-sm text-red-700 mt-1">{moduleOneError}</p>
                      <button
                        onClick={fetchModuleOneData}
                        className="mt-3 text-sm text-red-600 hover:text-red-800 font-medium"
                      >
                        Try Again
                      </button>
                    </div>
                  </div>
                </div>
              )}

              {moduleOneData && (
                <ModuleOne data={moduleOneData} />
              )}

              {/* Placeholder for other modules */}
              {moduleOneData && (
                <div className="bg-white rounded-lg shadow p-8">
                  <div className="text-center text-gray-500">
                    <p className="text-lg font-medium mb-2">More Modules Coming Soon</p>
                    <p className="text-sm">
                      Additional analysis modules will be displayed here as they become available.
                    </p>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </>
  );
};

export default ReportDetail;