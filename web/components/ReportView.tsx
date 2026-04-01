import React, { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import Module1Traffic from './modules/Module1Traffic';

interface ReportData {
  id: string;
  site_url: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  created_at: string;
  completed_at?: string;
  error?: string;
  modules: {
    module1_performance?: any;
    module2_page_triage?: any;
    module3_serp_landscape?: any;
    module4_content_intelligence?: any;
    module5_gameplan?: any;
    module6_algorithm_impact?: any;
    module7_intent_migration?: any;
    module8_link_architecture?: any;
    module9_seasonal_patterns?: any;
    module10_predictive_modeling?: any;
    module11_cross_dataset?: any;
    module12_executive_summary?: any;
  };
}

const ReportView: React.FC = () => {
  const { reportId } = useParams<{ reportId: string }>();
  const [report, setReport] = useState<ReportData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pollInterval, setPollInterval] = useState<NodeJS.Timeout | null>(null);
  const [activeModule, setActiveModule] = useState<number>(1);

  const fetchReport = async () => {
    try {
      const response = await fetch(`${process.env.REACT_APP_API_URL}/api/reports/${reportId}`, {
        headers: {
          'Authorization': `Bearer ${localStorage.getItem('access_token')}`,
        },
      });

      if (!response.ok) {
        if (response.status === 401) {
          // Token expired or invalid
          window.location.href = '/login';
          return;
        }
        if (response.status === 404) {
          throw new Error('Report not found');
        }
        throw new Error('Failed to fetch report');
      }

      const data: ReportData = await response.json();
      setReport(data);
      setError(null);

      // If report is completed or failed, stop polling
      if (data.status === 'completed' || data.status === 'failed') {
        if (pollInterval) {
          clearInterval(pollInterval);
          setPollInterval(null);
        }
        setLoading(false);
      }

      // If failed, set error
      if (data.status === 'failed') {
        setError(data.error || 'Report generation failed');
      }
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
    // Initial fetch
    fetchReport();

    // Set up polling for pending/processing reports
    const interval = setInterval(() => {
      if (report && (report.status === 'pending' || report.status === 'processing')) {
        fetchReport();
      }
    }, 5000); // Poll every 5 seconds

    setPollInterval(interval);

    return () => {
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [reportId]);

  const modulesList = [
    { id: 1, name: 'Health & Trajectory', available: true },
    { id: 2, name: 'Page-Level Triage', available: true },
    { id: 3, name: 'SERP Landscape', available: false },
    { id: 4, name: 'Content Intelligence', available: false },
    { id: 5, name: 'The Gameplan', available: true },
    { id: 6, name: 'Algorithm Impact', available: false },
    { id: 7, name: 'Intent Migration', available: false },
    { id: 8, name: 'Link Architecture', available: false },
    { id: 9, name: 'Seasonal Patterns', available: false },
    { id: 10, name: 'Predictive Modeling', available: false },
    { id: 11, name: 'Cross-Dataset Insights', available: false },
    { id: 12, name: 'Executive Summary', available: false },
  ];

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <h2 className="text-xl font-semibold text-gray-900 mb-2">
            {report?.status === 'processing' ? 'Generating Report...' : 'Loading Report...'}
          </h2>
          <p className="text-gray-600">
            {report?.status === 'processing' 
              ? 'This usually takes 2-5 minutes. Feel free to grab a coffee!' 
              : 'Please wait while we load your report.'}
          </p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="max-w-md w-full bg-white shadow-lg rounded-lg p-6">
          <div className="flex items-center justify-center w-12 h-12 mx-auto bg-red-100 rounded-full mb-4">
            <svg className="w-6 h-6 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </div>
          <h2 className="text-xl font-semibold text-gray-900 text-center mb-2">Error Loading Report</h2>
          <p className="text-gray-600 text-center mb-4">{error}</p>
          <button
            onClick={() => window.location.href = '/dashboard'}
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

  const renderModuleContent = () => {
    switch (activeModule) {
      case 1:
        return report.modules.module1_performance ? (
          <Module1Traffic data={report.modules.module1_performance} />
        ) : (
          <div className="text-center py-12">
            <p className="text-gray-600">Module data not available</p>
          </div>
        );
      case 2:
        return report.modules.module2_page_triage ? (
          <div className="bg-white rounded-lg shadow-sm p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-6">Page-Level Triage</h2>
            <div className="space-y-6">
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                <h3 className="font-semibold text-blue-900 mb-2">Summary</h3>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                  <div>
                    <div className="text-blue-600 font-medium">Growing</div>
                    <div className="text-2xl font-bold text-blue-900">
                      {report.modules.module2_page_triage.summary?.growing || 0}
                    </div>
                  </div>
                  <div>
                    <div className="text-green-600 font-medium">Stable</div>
                    <div className="text-2xl font-bold text-green-900">
                      {report.modules.module2_page_triage.summary?.stable || 0}
                    </div>
                  </div>
                  <div>
                    <div className="text-orange-600 font-medium">Decaying</div>
                    <div className="text-2xl font-bold text-orange-900">
                      {report.modules.module2_page_triage.summary?.decaying || 0}
                    </div>
                  </div>
                  <div>
                    <div className="text-red-600 font-medium">Critical</div>
                    <div className="text-2xl font-bold text-red-900">
                      {report.modules.module2_page_triage.summary?.critical || 0}
                    </div>
                  </div>
                </div>
              </div>
              <p className="text-gray-600">Detailed page analysis and priority recommendations coming soon.</p>
            </div>
          </div>
        ) : (
          <div className="text-center py-12">
            <p className="text-gray-600">Module data not available</p>
          </div>
        );
      case 5:
        return report.modules.module5_gameplan ? (
          <div className="bg-white rounded-lg shadow-sm p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-6">The Gameplan</h2>
            <div className="space-y-6">
              {report.modules.module5_gameplan.narrative && (
                <div className="prose max-w-none">
                  <p className="text-gray-700 leading-relaxed">{report.modules.module5_gameplan.narrative}</p>
                </div>
              )}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                  <h3 className="font-semibold text-red-900 mb-3 flex items-center">
                    <span className="bg-red-600 text-white rounded-full w-6 h-6 flex items-center justify-center text-sm mr-2">
                      {report.modules.module5_gameplan.critical?.length || 0}
                    </span>
                    Critical (This Week)
                  </h3>
                  <p className="text-sm text-red-700">High-impact fixes needed immediately</p>
                </div>
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <h3 className="font-semibold text-yellow-900 mb-3 flex items-center">
                    <span className="bg-yellow-600 text-white rounded-full w-6 h-6 flex items-center justify-center text-sm mr-2">
                      {report.modules.module5_gameplan.quick_wins?.length || 0}
                    </span>
                    Quick Wins (This Month)
                  </h3>
                  <p className="text-sm text-yellow-700">Fast, high-return opportunities</p>
                </div>
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                  <h3 className="font-semibold text-blue-900 mb-3 flex items-center">
                    <span className="bg-blue-600 text-white rounded-full w-6 h-6 flex items-center justify-center text-sm mr-2">
                      {report.modules.module5_gameplan.strategic?.length || 0}
                    </span>
                    Strategic (This Quarter)
                  </h3>
                  <p className="text-sm text-blue-700">Long-term growth initiatives</p>
                </div>
                <div className="bg-purple-50 border border-purple-200 rounded-lg p-4">
                  <h3 className="font-semibold text-purple-900 mb-3 flex items-center">
                    <span className="bg-purple-600 text-white rounded-full w-6 h-6 flex items-center justify-center text-sm mr-2">
                      {report.modules.module5_gameplan.structural?.length || 0}
                    </span>
                    Structural (Ongoing)
                  </h3>
                  <p className="text-sm text-purple-700">Foundation improvements</p>
                </div>
              </div>
              {(report.modules.module5_gameplan.total_estimated_monthly_click_recovery || 
                report.modules.module5_gameplan.total_estimated_monthly_click_growth) && (
                <div className="bg-green-50 border border-green-200 rounded-lg p-4">
                  <h3 className="font-semibold text-green-900 mb-3">Estimated Impact</h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                    {report.modules.module5_gameplan.total_estimated_monthly_click_recovery && (
                      <div>
                        <div className="text-green-700">Recoverable Clicks/Month</div>
                        <div className="text-2xl font-bold text-green-900">
                          {report.modules.module5_gameplan.total_estimated_monthly_click_recovery.toLocaleString()}
                        </div>
                      </div>
                    )}
                    {report.modules.module5_gameplan.total_estimated_monthly_click_growth && (
                      <div>
                        <div className="text-green-700">Growth Potential Clicks/Month</div>
                        <div className="text-2xl font-bold text-green-900">
                          {report.modules.module5_gameplan.total_estimated_monthly_click_growth.toLocaleString()}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        ) : (
          <div className="text-center py-12">
            <p className="text-gray-600">Module data not available</p>
          </div>
        );
      default:
        return (
          <div className="bg-white rounded-lg shadow-sm p-12 text-center">
            <div className="inline-flex items-center justify-center w-16 h-16 bg-gray-100 rounded-full mb-4">
              <svg className="w-8 h-8 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6v6m0 0v6m0-6h6m-6 0H6" />
              </svg>
            </div>
            <h3 className="text-xl font-semibold text-gray-900 mb-2">Coming Soon</h3>
            <p className="text-gray-600">This module is under development and will be available soon.</p>
          </div>
        );
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Search Intelligence Report</h1>
              <p className="text-sm text-gray-600 mt-1">{report.site_url}</p>
            </div>
            <div className="flex items-center space-x-4">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-green-100 text-green-800">
                <span className="w-2 h-2 bg-green-600 rounded-full mr-2"></span>
                Completed
              </span>
              <button
                onClick={() => window.location.href = '/dashboard'}
                className="text-gray-600 hover:text-gray-900"
              >
                <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="flex gap-8">
          {/* Sidebar Navigation */}
          <div className="w-64 flex-shrink-0">
            <div className="bg-white rounded-lg shadow-sm p-4 sticky top-8">
              <h3 className="text-sm font-semibold text-gray-900 uppercase tracking-wider mb-4">
                Modules
              </h3>
              <nav className="space-y-1">
                {modulesList.map((module) => (
                  <button
                    key={module.id}
                    onClick={() => module.available && setActiveModule(module.id)}
                    disabled={!module.available}
                    className={`w-full text-left px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                      activeModule === module.id
                        ? 'bg-blue-50 text-blue-700'
                        : module.available
                        ? 'text-gray-700 hover:bg-gray-50'
                        : 'text-gray-400 cursor-not-allowed'
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <span>{module.name}</span>
                      {!module.available && (
                        <span className="text-xs bg-gray-200 text-gray-600 px-2 py-0.5 rounded">
                          Soon
                        </span>
                      )}
                    </div>
                  </button>
                ))}
              </nav>
            </div>
          </div>

          {/* Module Content */}
          <div className="flex-1">
            {renderModuleContent()}
          </div>
        </div>
      </div>
    </div>
  );
};

export default ReportView;