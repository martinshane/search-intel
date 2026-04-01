import React, { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import Module1Performance from './Module1Performance';
import Module2PageTriage from './Module2PageTriage';
import Module3SerpLandscape from './Module3SerpLandscape';
import Module4ContentIntelligence from './Module4ContentIntelligence';
import Module5Gameplan from './Module5Gameplan';
import Module6AlgorithmImpact from './Module6AlgorithmImpact';
import Module7IntentMigration from './Module7IntentMigration';
import Module8LinkArchitecture from './Module8LinkArchitecture';
import Module9SeasonalPatterns from './Module9SeasonalPatterns';
import Module10PredictiveModeling from './Module10PredictiveModeling';
import Module11CrossDataset from './Module11CrossDataset';
import Module12ExecutiveSummary from './Module12ExecutiveSummary';

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

  const fetchReport = async () => {
    try {
      const response = await fetch(`${process.env.REACT_APP_API_URL}/api/reports/${reportId}`, {
        headers: {
          'Authorization': `Bearer ${localStorage.getItem('access_token')}`,
        },
      });

      if (!response.ok) {
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
    fetchReport();

    // Set up polling for in-progress reports
    const interval = setInterval(() => {
      if (report?.status === 'pending' || report?.status === 'processing') {
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

  const getStatusMessage = (status: string): string => {
    switch (status) {
      case 'pending':
        return 'Report queued for generation...';
      case 'processing':
        return 'Analyzing your search data... This may take 2-5 minutes.';
      case 'completed':
        return 'Report completed';
      case 'failed':
        return 'Report generation failed';
      default:
        return 'Unknown status';
    }
  };

  const getProgressPercentage = (): number => {
    if (!report) return 0;
    if (report.status === 'completed') return 100;
    if (report.status === 'failed') return 0;

    // Calculate progress based on completed modules
    const totalModules = 12;
    let completedModules = 0;

    if (report.modules.module1_performance) completedModules++;
    if (report.modules.module2_page_triage) completedModules++;
    if (report.modules.module3_serp_landscape) completedModules++;
    if (report.modules.module4_content_intelligence) completedModules++;
    if (report.modules.module5_gameplan) completedModules++;
    if (report.modules.module6_algorithm_impact) completedModules++;
    if (report.modules.module7_intent_migration) completedModules++;
    if (report.modules.module8_link_architecture) completedModules++;
    if (report.modules.module9_seasonal_patterns) completedModules++;
    if (report.modules.module10_predictive_modeling) completedModules++;
    if (report.modules.module11_cross_dataset) completedModules++;
    if (report.modules.module12_executive_summary) completedModules++;

    return Math.round((completedModules / totalModules) * 100);
  };

  if (loading && !report) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600 text-lg">Loading report...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center p-4">
        <div className="bg-white rounded-lg shadow-lg p-8 max-w-md w-full">
          <div className="text-red-600 mb-4">
            <svg className="w-16 h-16 mx-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h2 className="text-2xl font-bold text-gray-900 mb-2 text-center">Error</h2>
          <p className="text-gray-600 text-center mb-6">{error}</p>
          <button
            onClick={() => window.location.href = '/dashboard'}
            className="w-full bg-blue-600 text-white py-2 px-4 rounded-lg hover:bg-blue-700 transition-colors"
          >
            Return to Dashboard
          </button>
        </div>
      </div>
    );
  }

  if (!report) {
    return null;
  }

  if (report.status === 'pending' || report.status === 'processing') {
    const progress = getProgressPercentage();

    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center p-4">
        <div className="bg-white rounded-lg shadow-lg p-8 max-w-2xl w-full">
          <div className="text-center mb-6">
            <div className="animate-pulse mb-4">
              <svg className="w-16 h-16 mx-auto text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
              </svg>
            </div>
            <h2 className="text-2xl font-bold text-gray-900 mb-2">Generating Your Report</h2>
            <p className="text-gray-600">{getStatusMessage(report.status)}</p>
          </div>

          <div className="mb-6">
            <div className="flex justify-between items-center mb-2">
              <span className="text-sm font-medium text-gray-700">Progress</span>
              <span className="text-sm font-medium text-gray-700">{progress}%</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-3">
              <div
                className="bg-blue-600 h-3 rounded-full transition-all duration-500 ease-out"
                style={{ width: `${progress}%` }}
              ></div>
            </div>
          </div>

          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
            <p className="text-sm text-blue-800">
              <strong>What's happening:</strong> We're analyzing 12-16 months of your Google Search Console and GA4 data, 
              pulling live SERP data, running statistical models, and generating actionable insights. This typically takes 2-5 minutes.
            </p>
          </div>
        </div>
      </div>
    );
  }

  // Report is completed - render all modules
  const modules = report.modules;

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">Search Intelligence Report</h1>
              <p className="text-gray-600 mt-1">{report.site_url}</p>
              <p className="text-sm text-gray-500 mt-1">
                Generated on {new Date(report.completed_at || report.created_at).toLocaleDateString('en-US', {
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
                className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors flex items-center gap-2"
              >
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 17h2a2 2 0 002-2v-4a2 2 0 00-2-2H5a2 2 0 00-2 2v4a2 2 0 002 2h2m2 4h6a2 2 0 002-2v-4a2 2 0 00-2-2H9a2 2 0 00-2 2v4a2 2 0 002 2zm8-12V5a2 2 0 00-2-2H9a2 2 0 00-2 2v4h10z" />
                </svg>
                Print
              </button>
              <button
                onClick={() => window.location.href = '/dashboard'}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
              >
                Back to Dashboard
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Report Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Executive Summary - Module 12 (shown first) */}
        {modules.module12_executive_summary && (
          <section className="mb-8">
            <Module12ExecutiveSummary data={modules.module12_executive_summary} />
          </section>
        )}

        {/* The Gameplan - Module 5 (shown second for immediate action items) */}
        {modules.module5_gameplan && (
          <section className="mb-8">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-4">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">The Gameplan</h2>
              <p className="text-gray-600">Prioritized action items based on comprehensive analysis</p>
            </div>
            <Module5Gameplan data={modules.module5_gameplan} />
          </section>
        )}

        {/* Module 1: Health & Trajectory */}
        {modules.module1_performance && (
          <section className="mb-8">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-4">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">1. Health & Trajectory</h2>
              <p className="text-gray-600">Overall site performance trends, seasonality, and forecasts</p>
            </div>
            <Module1Performance data={modules.module1_performance} />
          </section>
        )}

        {/* Module 2: Page-Level Triage */}
        {modules.module2_page_triage && (
          <section className="mb-8">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-4">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">2. Page-Level Triage</h2>
              <p className="text-gray-600">Individual page performance analysis and decay detection</p>
            </div>
            <Module2PageTriage data={modules.module2_page_triage} />
          </section>
        )}

        {/* Module 3: SERP Landscape Analysis */}
        {modules.module3_serp_landscape && (
          <section className="mb-8">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-4">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">3. SERP Landscape Analysis</h2>
              <p className="text-gray-600">Search result features, competitors, and click share opportunities</p>
            </div>
            <Module3SerpLandscape data={modules.module3_serp_landscape} />
          </section>
        )}

        {/* Module 4: Content Intelligence */}
        {modules.module4_content_intelligence && (
          <section className="mb-8">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-4">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">4. Content Intelligence</h2>
              <p className="text-gray-600">Cannibalization, content gaps, and optimization opportunities</p>
            </div>
            <Module4ContentIntelligence data={modules.module4_content_intelligence} />
          </section>
        )}

        {/* Module 6: Algorithm Update Impact */}
        {modules.module6_algorithm_impact && (
          <section className="mb-8">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-4">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">6. Algorithm Update Impact Analysis</h2>
              <p className="text-gray-600">How Google updates have affected your site over time</p>
            </div>
            <Module6AlgorithmImpact data={modules.module6_algorithm_impact} />
          </section>
        )}

        {/* Module 7: Query Intent Migration */}
        {modules.module7_intent_migration && (
          <section className="mb-8">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-4">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">7. Query Intent Migration Tracking</h2>
              <p className="text-gray-600">How search intent patterns are changing for your keywords</p>
            </div>
            <Module7IntentMigration data={modules.module7_intent_migration} />
          </section>
        )}

        {/* Module 8: Link Architecture */}
        {modules.module8_link_architecture && (
          <section className="mb-8">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-4">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">8. Internal Link Architecture</h2>
              <p className="text-gray-600">Link structure analysis and PageRank flow optimization</p>
            </div>
            <Module8LinkArchitecture data={modules.module8_link_architecture} />
          </section>
        )}

        {/* Module 9: Seasonal Patterns */}
        {modules.module9_seasonal_patterns && (
          <section className="mb-8">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-4">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">9. Seasonal Pattern Intelligence</h2>
              <p className="text-gray-600">Cyclical trends and content calendar recommendations</p>
            </div>
            <Module9SeasonalPatterns data={modules.module9_seasonal_patterns} />
          </section>
        )}

        {/* Module 10: Predictive Modeling */}
        {modules.module10_predictive_modeling && (
          <section className="mb-8">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-4">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">10. Predictive Modeling</h2>
              <p className="text-gray-600">Traffic forecasts and scenario planning</p>
            </div>
            <Module10PredictiveModeling data={modules.module10_predictive_modeling} />
          </section>
        )}

        {/* Module 11: Cross-Dataset Insights */}
        {modules.module11_cross_dataset && (
          <section className="mb-8">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-4">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">11. Cross-Dataset Correlation Analysis</h2>
              <p className="text-gray-600">Relationships between search performance and engagement metrics</p>
            </div>
            <Module11CrossDataset data={modules.module11_cross_dataset} />
          </section>
        )}

        {/* Footer */}
        <div className="mt-12 border-t border-gray-200 pt-8 pb-4">
          <div className="text-center text-gray-500 text-sm">
            <p>This report was generated using 12-16 months of Google Search Console and GA4 data.</p>
            <p className="mt-2">For questions or consultation, contact your search intelligence provider.</p>
            <p className="mt-4 text-xs">Report ID: {report.id}</p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ReportView;
