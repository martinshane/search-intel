'use client';

import React, { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
import Module1TrafficOverview from './modules/Module1TrafficOverview';

interface ReportData {
  id: string;
  siteUrl: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  createdAt: string;
  completedAt?: string;
  modules: {
    module1_health_trajectory?: any;
    module2_page_triage?: any;
    module3_serp_landscape?: any;
    module4_content_intelligence?: any;
    module5_gameplan?: any;
    module6_algorithm_impacts?: any;
    module7_intent_migration?: any;
    module8_internal_link_equity?: any;
    module9_cross_dataset_correlation?: any;
    module10_predictive_modeling?: any;
    module11_competitive_moat?: any;
    module12_roi_estimation?: any;
  };
}

type ModuleKey = 
  | 'module1_health_trajectory'
  | 'module2_page_triage'
  | 'module3_serp_landscape'
  | 'module4_content_intelligence'
  | 'module5_gameplan'
  | 'module6_algorithm_impacts'
  | 'module7_intent_migration'
  | 'module8_internal_link_equity'
  | 'module9_cross_dataset_correlation'
  | 'module10_predictive_modeling'
  | 'module11_competitive_moat'
  | 'module12_roi_estimation';

const MODULE_LABELS: Record<ModuleKey, string> = {
  module1_health_trajectory: 'Health & Trajectory',
  module2_page_triage: 'Page-Level Triage',
  module3_serp_landscape: 'SERP Landscape',
  module4_content_intelligence: 'Content Intelligence',
  module5_gameplan: 'The Gameplan',
  module6_algorithm_impacts: 'Algorithm Impact',
  module7_intent_migration: 'Intent Migration',
  module8_internal_link_equity: 'Internal Link Equity',
  module9_cross_dataset_correlation: 'Cross-Dataset Correlation',
  module10_predictive_modeling: 'Predictive Modeling',
  module11_competitive_moat: 'Competitive Moat',
  module12_roi_estimation: 'ROI Estimation',
};

export default function ReportViewer() {
  const params = useParams();
  const reportId = params?.reportId as string;
  
  const [reportData, setReportData] = useState<ReportData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeModule, setActiveModule] = useState<ModuleKey>('module1_health_trajectory');
  const [pollInterval, setPollInterval] = useState<NodeJS.Timeout | null>(null);

  const fetchReportData = async () => {
    try {
      const response = await fetch(`/api/report/${reportId}`);
      
      if (!response.ok) {
        if (response.status === 404) {
          throw new Error('Report not found');
        }
        throw new Error('Failed to fetch report data');
      }
      
      const data = await response.json();
      setReportData(data);
      setError(null);
      
      // Stop polling if report is completed or failed
      if (data.status === 'completed' || data.status === 'failed') {
        if (pollInterval) {
          clearInterval(pollInterval);
          setPollInterval(null);
        }
      }
      
      setLoading(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      setLoading(false);
      if (pollInterval) {
        clearInterval(pollInterval);
        setPollInterval(null);
      }
    }
  };

  useEffect(() => {
    if (!reportId) {
      setError('No report ID provided');
      setLoading(false);
      return;
    }

    // Initial fetch
    fetchReportData();

    // Set up polling for pending/processing reports
    const interval = setInterval(() => {
      fetchReportData();
    }, 5000); // Poll every 5 seconds

    setPollInterval(interval);

    return () => {
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [reportId]);

  const renderModuleContent = () => {
    if (!reportData || !reportData.modules) {
      return null;
    }

    const moduleData = reportData.modules[activeModule];

    switch (activeModule) {
      case 'module1_health_trajectory':
        if (moduleData) {
          return <Module1TrafficOverview data={moduleData} />;
        }
        return <PlaceholderModule moduleName="Health & Trajectory" />;
      
      case 'module2_page_triage':
        if (moduleData) {
          return <PlaceholderModule moduleName="Page-Level Triage" data={moduleData} />;
        }
        return <PlaceholderModule moduleName="Page-Level Triage" />;
      
      case 'module3_serp_landscape':
        if (moduleData) {
          return <PlaceholderModule moduleName="SERP Landscape" data={moduleData} />;
        }
        return <PlaceholderModule moduleName="SERP Landscape" />;
      
      case 'module4_content_intelligence':
        if (moduleData) {
          return <PlaceholderModule moduleName="Content Intelligence" data={moduleData} />;
        }
        return <PlaceholderModule moduleName="Content Intelligence" />;
      
      case 'module5_gameplan':
        if (moduleData) {
          return <PlaceholderModule moduleName="The Gameplan" data={moduleData} />;
        }
        return <PlaceholderModule moduleName="The Gameplan" />;
      
      case 'module6_algorithm_impacts':
        if (moduleData) {
          return <PlaceholderModule moduleName="Algorithm Impact" data={moduleData} />;
        }
        return <PlaceholderModule moduleName="Algorithm Impact" />;
      
      case 'module7_intent_migration':
        if (moduleData) {
          return <PlaceholderModule moduleName="Intent Migration" data={moduleData} />;
        }
        return <PlaceholderModule moduleName="Intent Migration" />;
      
      case 'module8_internal_link_equity':
        if (moduleData) {
          return <PlaceholderModule moduleName="Internal Link Equity" data={moduleData} />;
        }
        return <PlaceholderModule moduleName="Internal Link Equity" />;
      
      case 'module9_cross_dataset_correlation':
        if (moduleData) {
          return <PlaceholderModule moduleName="Cross-Dataset Correlation" data={moduleData} />;
        }
        return <PlaceholderModule moduleName="Cross-Dataset Correlation" />;
      
      case 'module10_predictive_modeling':
        if (moduleData) {
          return <PlaceholderModule moduleName="Predictive Modeling" data={moduleData} />;
        }
        return <PlaceholderModule moduleName="Predictive Modeling" />;
      
      case 'module11_competitive_moat':
        if (moduleData) {
          return <PlaceholderModule moduleName="Competitive Moat" data={moduleData} />;
        }
        return <PlaceholderModule moduleName="Competitive Moat" />;
      
      case 'module12_roi_estimation':
        if (moduleData) {
          return <PlaceholderModule moduleName="ROI Estimation" data={moduleData} />;
        }
        return <PlaceholderModule moduleName="ROI Estimation" />;
      
      default:
        return <PlaceholderModule moduleName="Unknown Module" />;
    }
  };

  if (loading && !reportData) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <h2 className="text-xl font-semibold text-gray-800">Loading Report...</h2>
          <p className="text-gray-600 mt-2">Please wait while we fetch your data</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="bg-white rounded-lg shadow-lg p-8 max-w-md w-full">
          <div className="text-red-600 text-center mb-4">
            <svg className="w-16 h-16 mx-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h2 className="text-2xl font-bold text-gray-800 text-center mb-2">Error Loading Report</h2>
          <p className="text-gray-600 text-center">{error}</p>
          <button
            onClick={() => window.location.reload()}
            className="mt-6 w-full bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700 transition"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!reportData) {
    return null;
  }

  const isProcessing = reportData.status === 'pending' || reportData.status === 'processing';
  const isFailed = reportData.status === 'failed';

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Search Intelligence Report</h1>
              <p className="text-sm text-gray-600 mt-1">{reportData.siteUrl}</p>
            </div>
            <div className="flex items-center gap-4">
              {isProcessing && (
                <div className="flex items-center gap-2 text-blue-600">
                  <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-blue-600"></div>
                  <span className="text-sm font-medium">Processing...</span>
                </div>
              )}
              {isFailed && (
                <div className="flex items-center gap-2 text-red-600">
                  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <span className="text-sm font-medium">Generation Failed</span>
                </div>
              )}
              {reportData.status === 'completed' && (
                <div className="flex items-center gap-2 text-green-600">
                  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                  </svg>
                  <span className="text-sm font-medium">Complete</span>
                </div>
              )}
              <button className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700 transition text-sm font-medium">
                Export PDF
              </button>
            </div>
          </div>
          
          {/* Report metadata */}
          <div className="mt-4 flex items-center gap-6 text-sm text-gray-600">
            <span>Generated: {new Date(reportData.createdAt).toLocaleString()}</span>
            {reportData.completedAt && (
              <span>Completed: {new Date(reportData.completedAt).toLocaleString()}</span>
            )}
          </div>
        </div>
      </header>

      <div className="flex">
        {/* Sidebar Navigation */}
        <aside className="w-64 bg-white border-r border-gray-200 min-h-[calc(100vh-80px)] sticky top-[80px] overflow-y-auto">
          <nav className="p-4">
            <h2 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">
              Report Modules
            </h2>
            <ul className="space-y-1">
              {(Object.keys(MODULE_LABELS) as ModuleKey[]).map((moduleKey) => {
                const isActive = activeModule === moduleKey;
                const hasData = reportData.modules && reportData.modules[moduleKey];
                
                return (
                  <li key={moduleKey}>
                    <button
                      onClick={() => setActiveModule(moduleKey)}
                      className={`w-full text-left px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                        isActive
                          ? 'bg-blue-50 text-blue-700'
                          : 'text-gray-700 hover:bg-gray-50'
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <span>{MODULE_LABELS[moduleKey]}</span>
                        {hasData && (
                          <svg className="w-4 h-4 text-green-500" fill="currentColor" viewBox="0 0 20 20">
                            <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                          </svg>
                        )}
                      </div>
                    </button>
                  </li>
                );
              })}
            </ul>
          </nav>
        </aside>

        {/* Main Content */}
        <main className="flex-1 p-8">
          {isProcessing && (
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 mb-6">
              <div className="flex items-start gap-4">
                <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600 mt-1"></div>
                <div>
                  <h3 className="text-lg font-semibold text-blue-900 mb-1">
                    Report Generation In Progress
                  </h3>
                  <p className="text-blue-700 text-sm">
                    This typically takes 2-5 minutes. We're analyzing your GSC and GA4 data,
                    fetching live SERP results, and running statistical models. This page will
                    auto-update as modules complete.
                  </p>
                </div>
              </div>
            </div>
          )}

          {isFailed && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-6 mb-6">
              <div className="flex items-start gap-4">
                <svg className="w-6 h-6 text-red-600 mt-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                <div>
                  <h3 className="text-lg font-semibold text-red-900 mb-1">
                    Report Generation Failed
                  </h3>
                  <p className="text-red-700 text-sm">
                    We encountered an error while generating your report. Please try again or contact support if the issue persists.
                  </p>
                </div>
              </div>
            </div>
          )}

          {renderModuleContent()}
        </main>
      </div>
    </div>
  );
}

function PlaceholderModule({ moduleName, data }: { moduleName: string; data?: any }) {
  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-8">
      <h2 className="text-2xl font-bold text-gray-900 mb-4">{moduleName}</h2>
      
      {!data ? (
        <div className="text-center py-12">
          <svg className="w-16 h-16 text-gray-300 mx-auto mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
          </svg>
          <h3 className="text-lg font-semibold text-gray-700 mb-2">Module Not Yet Available</h3>
          <p className="text-gray-600 max-w-md mx-auto">
            This module is still being generated. The page will automatically update when data is available.
          </p>
        </div>
      ) : (
        <div className="space-y-6">
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
            <p className="text-sm text-blue-800">
              <strong>Module Preview:</strong> This module has completed processing. Full visualization will be implemented in the next development phase.
            </p>
          </div>
          
          <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
            <h3 className="text-sm font-semibold text-gray-700 mb-3">Raw Module Data:</h3>
            <pre className="text-xs text-gray-600 overflow-x-auto whitespace-pre-wrap">
              {JSON.stringify(data, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}
