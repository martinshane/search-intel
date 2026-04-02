import React, { useEffect, useState } from 'react';
import Module1TrafficOverview from './Module1TrafficOverview';

interface ReportData {
  report_id: string;
  domain: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  created_at: string;
  completed_at?: string;
  error?: string;
  modules: {
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
}

interface ReportDisplayProps {
  reportId: string;
}

const ReportDisplay: React.FC<ReportDisplayProps> = ({ reportId }) => {
  const [reportData, setReportData] = useState<ReportData | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [pollingInterval, setPollingInterval] = useState<NodeJS.Timeout | null>(null);

  const fetchReport = async () => {
    try {
      const response = await fetch(`/api/report/${reportId}`);
      
      if (!response.ok) {
        if (response.status === 404) {
          throw new Error('Report not found');
        }
        throw new Error(`Failed to fetch report: ${response.statusText}`);
      }

      const data: ReportData = await response.json();
      setReportData(data);
      setError(null);

      // Stop polling if report is completed or failed
      if (data.status === 'completed' || data.status === 'failed') {
        if (pollingInterval) {
          clearInterval(pollingInterval);
          setPollingInterval(null);
        }
        
        if (data.status === 'failed') {
          setError(data.error || 'Report generation failed');
        }
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'An unknown error occurred';
      setError(errorMessage);
      
      // Stop polling on error
      if (pollingInterval) {
        clearInterval(pollingInterval);
        setPollingInterval(null);
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    // Initial fetch
    fetchReport();

    // Set up polling for pending/processing reports
    const interval = setInterval(() => {
      if (reportData?.status === 'pending' || reportData?.status === 'processing') {
        fetchReport();
      }
    }, 5000); // Poll every 5 seconds

    setPollingInterval(interval);

    // Cleanup
    return () => {
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [reportId]);

  // Re-enable polling if status changes back to pending/processing
  useEffect(() => {
    if (reportData?.status === 'pending' || reportData?.status === 'processing') {
      if (!pollingInterval) {
        const interval = setInterval(() => {
          fetchReport();
        }, 5000);
        setPollingInterval(interval);
      }
    }
  }, [reportData?.status]);

  if (loading && !reportData) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mb-4"></div>
          <p className="text-gray-600 text-lg">Loading report...</p>
        </div>
      </div>
    );
  }

  if (error && !reportData) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="bg-white rounded-lg shadow-lg p-8 max-w-md w-full">
          <div className="flex items-center justify-center w-12 h-12 mx-auto bg-red-100 rounded-full mb-4">
            <svg className="w-6 h-6 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </div>
          <h2 className="text-xl font-semibold text-gray-900 text-center mb-2">Error Loading Report</h2>
          <p className="text-gray-600 text-center mb-4">{error}</p>
          <button
            onClick={() => {
              setError(null);
              setLoading(true);
              fetchReport();
            }}
            className="w-full bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (reportData?.status === 'pending' || reportData?.status === 'processing') {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="bg-white rounded-lg shadow-lg p-8 max-w-md w-full">
          <div className="flex items-center justify-center mb-4">
            <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
          </div>
          <h2 className="text-xl font-semibold text-gray-900 text-center mb-2">
            {reportData.status === 'pending' ? 'Report Queued' : 'Generating Report'}
          </h2>
          <p className="text-gray-600 text-center mb-4">
            {reportData.status === 'pending' 
              ? 'Your report is queued for processing...' 
              : 'Analyzing your search data and generating insights...'}
          </p>
          <div className="bg-gray-200 rounded-full h-2 mb-2">
            <div className="bg-blue-600 h-2 rounded-full animate-pulse" style={{ width: '60%' }}></div>
          </div>
          <p className="text-sm text-gray-500 text-center">
            This typically takes 2-5 minutes
          </p>
        </div>
      </div>
    );
  }

  if (reportData?.status === 'failed') {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="bg-white rounded-lg shadow-lg p-8 max-w-md w-full">
          <div className="flex items-center justify-center w-12 h-12 mx-auto bg-red-100 rounded-full mb-4">
            <svg className="w-6 h-6 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </div>
          <h2 className="text-xl font-semibold text-gray-900 text-center mb-2">Report Generation Failed</h2>
          <p className="text-gray-600 text-center mb-4">
            {reportData.error || 'An error occurred while generating your report.'}
          </p>
          <button
            onClick={() => window.location.href = '/'}
            className="w-full bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors"
          >
            Return to Home
          </button>
        </div>
      </div>
    );
  }

  // Report completed successfully
  const hasModule1 = reportData?.modules?.module1 !== undefined && reportData?.modules?.module1 !== null;

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">Search Intelligence Report</h1>
              <p className="text-gray-600 mt-1">{reportData?.domain}</p>
            </div>
            <div className="text-right">
              <p className="text-sm text-gray-500">Generated</p>
              <p className="text-sm font-medium text-gray-900">
                {reportData?.completed_at 
                  ? new Date(reportData.completed_at).toLocaleDateString('en-US', {
                      year: 'numeric',
                      month: 'long',
                      day: 'numeric',
                      hour: '2-digit',
                      minute: '2-digit'
                    })
                  : new Date(reportData?.created_at || '').toLocaleDateString('en-US', {
                      year: 'numeric',
                      month: 'long',
                      day: 'numeric',
                      hour: '2-digit',
                      minute: '2-digit'
                    })
                }
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Report Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {hasModule1 ? (
          <div className="space-y-8">
            <Module1TrafficOverview data={reportData.modules.module1} />
            
            {/* Placeholder for other modules */}
            {!reportData.modules.module2 && !reportData.modules.module3 && (
              <div className="bg-white rounded-lg shadow p-6 text-center text-gray-500">
                Additional analysis modules will appear here as they become available
              </div>
            )}
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow p-12 text-center">
            <div className="max-w-md mx-auto">
              <svg className="w-16 h-16 text-gray-400 mx-auto mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
              </svg>
              <h3 className="text-lg font-medium text-gray-900 mb-2">Report Data Not Available</h3>
              <p className="text-gray-600">
                This report has been generated but does not contain any analysis data yet.
                Please check back later or contact support if this issue persists.
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportDisplay;
