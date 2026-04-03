'use client';

import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
import Module1QueryVisibility from '@/components/modules/Module1QueryVisibility';

interface Report {
  id: string;
  userId: string;
  domain: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  createdAt: string;
  updatedAt: string;
  completedAt?: string;
  errorMessage?: string;
}

interface ModuleData {
  moduleId: number;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  data?: any;
  errorMessage?: string;
  completedAt?: string;
}

export default function ReportPage() {
  const params = useParams();
  const reportId = params?.reportId as string;
  
  const [report, setReport] = useState<Report | null>(null);
  const [module1Data, setModule1Data] = useState<ModuleData | null>(null);
  const [isLoadingReport, setIsLoadingReport] = useState(true);
  const [isLoadingModule1, setIsLoadingModule1] = useState(true);
  const [reportError, setReportError] = useState<string | null>(null);
  const [module1Error, setModule1Error] = useState<string | null>(null);

  // Fetch report metadata
  useEffect(() => {
    if (!reportId) return;

    const fetchReport = async () => {
      try {
        setIsLoadingReport(true);
        setReportError(null);
        
        const response = await fetch(`/api/reports/${reportId}`);
        
        if (!response.ok) {
          if (response.status === 404) {
            setReportError('Report not found');
          } else {
            const errorData = await response.json().catch(() => ({}));
            setReportError(errorData.error || 'Failed to load report');
          }
          return;
        }

        const data = await response.json();
        setReport(data);
      } catch (error) {
        console.error('Error fetching report:', error);
        setReportError('Network error while loading report');
      } finally {
        setIsLoadingReport(false);
      }
    };

    fetchReport();
  }, [reportId]);

  // Fetch Module 1 data
  useEffect(() => {
    if (!reportId) return;

    const fetchModule1 = async () => {
      try {
        setIsLoadingModule1(true);
        setModule1Error(null);
        
        const response = await fetch(`/api/reports/${reportId}/modules/1`);
        
        if (!response.ok) {
          if (response.status === 404) {
            setModule1Error('Module data not found');
          } else {
            const errorData = await response.json().catch(() => ({}));
            setModule1Error(errorData.error || 'Failed to load module data');
          }
          return;
        }

        const data = await response.json();
        setModule1Data(data);
      } catch (error) {
        console.error('Error fetching module 1:', error);
        setModule1Error('Network error while loading module data');
      } finally {
        setIsLoadingModule1(false);
      }
    };

    fetchModule1();
  }, [reportId]);

  // Poll for updates if report or module is still processing
  useEffect(() => {
    if (!reportId) return;
    
    const shouldPoll = 
      (report && (report.status === 'pending' || report.status === 'processing')) ||
      (module1Data && (module1Data.status === 'pending' || module1Data.status === 'processing'));

    if (!shouldPoll) return;

    const pollInterval = setInterval(() => {
      // Re-fetch report if it's still processing
      if (report && (report.status === 'pending' || report.status === 'processing')) {
        fetch(`/api/reports/${reportId}`)
          .then(res => res.json())
          .then(data => setReport(data))
          .catch(console.error);
      }

      // Re-fetch module 1 if it's still processing
      if (module1Data && (module1Data.status === 'pending' || module1Data.status === 'processing')) {
        fetch(`/api/reports/${reportId}/modules/1`)
          .then(res => res.json())
          .then(data => setModule1Data(data))
          .catch(console.error);
      }
    }, 5000); // Poll every 5 seconds

    return () => clearInterval(pollInterval);
  }, [reportId, report, module1Data]);

  // Loading state for initial report fetch
  if (isLoadingReport) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading report...</p>
        </div>
      </div>
    );
  }

  // Error state for report fetch
  if (reportError) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center max-w-md">
          <div className="bg-red-100 border border-red-400 text-red-700 px-6 py-4 rounded-lg">
            <h2 className="text-xl font-bold mb-2">Error Loading Report</h2>
            <p>{reportError}</p>
          </div>
          <a
            href="/dashboard"
            className="mt-6 inline-block px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            Return to Dashboard
          </a>
        </div>
      </div>
    );
  }

  // Report not found
  if (!report) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center max-w-md">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Report Not Found</h2>
          <p className="text-gray-600 mb-6">
            The requested report could not be found.
          </p>
          <a
            href="/dashboard"
            className="inline-block px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            Return to Dashboard
          </a>
        </div>
      </div>
    );
  }

  // Report failed state
  if (report.status === 'failed') {
    return (
      <div className="min-h-screen bg-gray-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="bg-white rounded-lg shadow-sm p-8">
            <div className="text-center">
              <div className="text-red-500 mb-4">
                <svg className="w-16 h-16 mx-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
              </div>
              <h2 className="text-2xl font-bold text-gray-800 mb-2">Report Generation Failed</h2>
              <p className="text-gray-600 mb-4">
                {report.errorMessage || 'An error occurred while generating the report.'}
              </p>
              <div className="flex gap-4 justify-center">
                <a
                  href="/dashboard"
                  className="px-6 py-2 bg-gray-200 text-gray-800 rounded-lg hover:bg-gray-300 transition-colors"
                >
                  Return to Dashboard
                </a>
                <button
                  onClick={() => window.location.reload()}
                  className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                >
                  Retry
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // Report pending/processing state
  if (report.status === 'pending' || report.status === 'processing') {
    return (
      <div className="min-h-screen bg-gray-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="bg-white rounded-lg shadow-sm p-8">
            <div className="text-center">
              <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-blue-600 mx-auto mb-6"></div>
              <h2 className="text-2xl font-bold text-gray-800 mb-2">
                {report.status === 'pending' ? 'Report Queued' : 'Generating Report'}
              </h2>
              <p className="text-gray-600 mb-6">
                {report.status === 'pending' 
                  ? 'Your report is queued for processing. This usually takes a few seconds...'
                  : 'Analyzing your search data. This may take 2-5 minutes...'}
              </p>
              <div className="max-w-md mx-auto">
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-left">
                  <h3 className="font-semibold text-blue-900 mb-2">What we're doing:</h3>
                  <ul className="text-sm text-blue-800 space-y-1">
                    <li>• Fetching Google Search Console data</li>
                    <li>• Fetching Google Analytics 4 data</li>
                    <li>• Analyzing search performance trends</li>
                    <li>• Detecting ranking patterns and anomalies</li>
                    <li>• Generating insights and recommendations</li>
                  </ul>
                </div>
              </div>
              <p className="text-sm text-gray-500 mt-6">
                This page will automatically update when processing is complete.
              </p>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // Report completed - render modules
  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Report Header */}
        <div className="bg-white rounded-lg shadow-sm p-6 mb-6">
          <div className="flex items-start justify-between">
            <div>
              <h1 className="text-3xl font-bold text-gray-900 mb-2">
                Search Intelligence Report
              </h1>
              <p className="text-gray-600">{report.domain}</p>
              <p className="text-sm text-gray-500 mt-1">
                Generated {new Date(report.completedAt || report.createdAt).toLocaleDateString('en-US', {
                  year: 'numeric',
                  month: 'long',
                  day: 'numeric',
                  hour: '2-digit',
                  minute: '2-digit'
                })}
              </p>
            </div>
            <a
              href="/dashboard"
              className="px-4 py-2 text-sm text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200 transition-colors"
            >
              ← Back to Dashboard
            </a>
          </div>
        </div>

        {/* Module 1: Query Visibility & Trajectory */}
        <div className="mb-6">
          {isLoadingModule1 && (
            <div className="bg-white rounded-lg shadow-sm p-8">
              <div className="text-center">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
                <p className="text-gray-600">Loading module data...</p>
              </div>
            </div>
          )}

          {!isLoadingModule1 && module1Error && (
            <div className="bg-white rounded-lg shadow-sm p-8">
              <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                <div className="flex items-start">
                  <svg className="w-5 h-5 text-yellow-600 mr-3 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                  </svg>
                  <div>
                    <h3 className="font-semibold text-yellow-900">Module Data Unavailable</h3>
                    <p className="text-sm text-yellow-800 mt-1">{module1Error}</p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {!isLoadingModule1 && module1Data && module1Data.status === 'pending' && (
            <div className="bg-white rounded-lg shadow-sm p-8">
              <div className="text-center">
                <div className="w-12 h-12 bg-gray-200 rounded-full mx-auto mb-4 flex items-center justify-center">
                  <svg className="w-6 h-6 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                </div>
                <h3 className="text-lg font-semibold text-gray-800 mb-2">Module 1: Query Visibility & Trajectory</h3>
                <p className="text-gray-600">Queued for processing...</p>
              </div>
            </div>
          )}

          {!isLoadingModule1 && module1Data && module1Data.status === 'processing' && (
            <div className="bg-white rounded-lg shadow-sm p-8">
              <div className="text-center">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
                <h3 className="text-lg font-semibold text-gray-800 mb-2">Module 1: Query Visibility & Trajectory</h3>
                <p className="text-gray-600">Analyzing search performance trends...</p>
              </div>
            </div>
          )}

          {!isLoadingModule1 && module1Data && module1Data.status === 'failed' && (
            <div className="bg-white rounded-lg shadow-sm p-8">
              <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                <div className="flex items-start">
                  <svg className="w-5 h-5 text-red-600 mr-3 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <div>
                    <h3 className="font-semibold text-red-900">Module 1: Processing Failed</h3>
                    <p className="text-sm text-red-800 mt-1">
                      {module1Data.errorMessage || 'An error occurred while processing this module.'}
                    </p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {!isLoadingModule1 && module1Data && module1Data.status === 'completed' && module1Data.data && (
            <Module1QueryVisibility data={module1Data.data} />
          )}
        </div>

        {/* Placeholder for future modules */}
        <div className="bg-white rounded-lg shadow-sm p-8">
          <div className="text-center text-gray-500">
            <svg className="w-12 h-12 mx-auto mb-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
            </svg>
            <p className="text-sm">Additional analysis modules coming soon</p>
          </div>
        </div>
      </div>
    </div>
  );
}
