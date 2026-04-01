import React, { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import Module1TrafficOverview from './modules/Module1TrafficOverview';

interface ReportData {
  id: string;
  site_url: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  created_at: string;
  completed_at?: string;
  error_message?: string;
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

const Report: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [report, setReport] = useState<ReportData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeModule, setActiveModule] = useState<string>('module1');

  useEffect(() => {
    if (!id) {
      setError('No report ID provided');
      setLoading(false);
      return;
    }

    fetchReport();
    const interval = setInterval(() => {
      if (report?.status === 'processing' || report?.status === 'pending') {
        fetchReport();
      }
    }, 5000);

    return () => clearInterval(interval);
  }, [id]);

  const fetchReport = async () => {
    try {
      const response = await fetch(`/api/reports/${id}`);
      if (!response.ok) {
        if (response.status === 404) {
          throw new Error('Report not found');
        }
        throw new Error('Failed to fetch report');
      }
      const data = await response.json();
      setReport(data);
      setLoading(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      setLoading(false);
    }
  };

  const handlePrint = () => {
    window.print();
  };

  const handleExport = () => {
    alert('Export functionality coming soon');
  };

  const scrollToModule = (moduleId: string) => {
    setActiveModule(moduleId);
    const element = document.getElementById(moduleId);
    if (element) {
      element.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mb-4"></div>
          <p className="text-gray-600 text-lg">Loading report...</p>
          {report?.status === 'processing' && (
            <p className="text-gray-500 text-sm mt-2">
              Report generation in progress. This may take 2-5 minutes.
            </p>
          )}
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center max-w-md">
          <div className="bg-red-50 border border-red-200 rounded-lg p-6">
            <svg
              className="mx-auto h-12 w-12 text-red-600 mb-4"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
              />
            </svg>
            <h2 className="text-xl font-semibold text-gray-900 mb-2">Error Loading Report</h2>
            <p className="text-gray-600 mb-4">{error}</p>
            <button
              onClick={() => navigate('/dashboard')}
              className="bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700 transition"
            >
              Back to Dashboard
            </button>
          </div>
        </div>
      </div>
    );
  }

  if (!report) {
    return null;
  }

  if (report.status === 'failed') {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center max-w-md">
          <div className="bg-red-50 border border-red-200 rounded-lg p-6">
            <h2 className="text-xl font-semibold text-gray-900 mb-2">Report Generation Failed</h2>
            <p className="text-gray-600 mb-4">
              {report.error_message || 'An error occurred while generating the report.'}
            </p>
            <button
              onClick={() => navigate('/dashboard')}
              className="bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700 transition"
            >
              Back to Dashboard
            </button>
          </div>
        </div>
      </div>
    );
  }

  const modules = [
    { id: 'module1', name: 'Health & Trajectory', component: Module1TrafficOverview },
    { id: 'module2', name: 'Page-Level Triage', component: null },
    { id: 'module3', name: 'SERP Landscape Analysis', component: null },
    { id: 'module4', name: 'Content Intelligence', component: null },
    { id: 'module5', name: 'The Gameplan', component: null },
    { id: 'module6', name: 'Algorithm Update Impact', component: null },
    { id: 'module7', name: 'Query Intent Migration', component: null },
    { id: 'module8', name: 'Technical SEO Health', component: null },
    { id: 'module9', name: 'Backlink Intelligence', component: null },
    { id: 'module10', name: 'Competitive Intelligence', component: null },
    { id: 'module11', name: 'Predictive Models', component: null },
    { id: 'module12', name: 'Executive Summary', component: null },
  ];

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-40 print:relative">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center">
              <button
                onClick={() => navigate('/dashboard')}
                className="mr-4 text-gray-400 hover:text-gray-600 print:hidden"
              >
                <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M10 19l-7-7m0 0l7-7m-7 7h18"
                  />
                </svg>
              </button>
              <div>
                <h1 className="text-xl font-bold text-gray-900">Search Intelligence Report</h1>
                <p className="text-sm text-gray-500">{report.site_url}</p>
              </div>
            </div>
            <div className="flex items-center space-x-3 print:hidden">
              <button
                onClick={handlePrint}
                className="bg-white border border-gray-300 text-gray-700 px-4 py-2 rounded-lg hover:bg-gray-50 transition flex items-center"
              >
                <svg className="h-4 w-4 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M17 17h2a2 2 0 002-2v-4a2 2 0 00-2-2H5a2 2 0 00-2 2v4a2 2 0 002 2h2m2 4h6a2 2 0 002-2v-4a2 2 0 00-2-2H9a2 2 0 00-2 2v4a2 2 0 002 2zm8-12V5a2 2 0 00-2-2H9a2 2 0 00-2 2v4h10z"
                  />
                </svg>
                Print
              </button>
              <button
                onClick={handleExport}
                className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition flex items-center"
              >
                <svg className="h-4 w-4 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
                  />
                </svg>
                Export PDF
              </button>
            </div>
          </div>
        </div>
      </header>

      <div className="flex max-w-7xl mx-auto">
        {/* Sidebar Navigation */}
        <aside className="w-64 flex-shrink-0 print:hidden">
          <nav className="sticky top-20 p-6">
            <ul className="space-y-1">
              {modules.map((module, index) => (
                <li key={module.id}>
                  <button
                    onClick={() => scrollToModule(module.id)}
                    className={`w-full text-left px-4 py-2 rounded-lg transition ${
                      activeModule === module.id
                        ? 'bg-blue-50 text-blue-700 font-medium'
                        : 'text-gray-600 hover:bg-gray-50'
                    }`}
                  >
                    <span className="text-sm font-semibold text-gray-400 mr-3">
                      {String(index + 1).padStart(2, '0')}
                    </span>
                    {module.name}
                  </button>
                </li>
              ))}
            </ul>
          </nav>
        </aside>

        {/* Main Content */}
        <main className="flex-1 px-4 sm:px-6 lg:px-8 py-8">
          {/* Report Metadata */}
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-6 print:shadow-none print:border-0">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div>
                <p className="text-sm text-gray-500 mb-1">Report Generated</p>
                <p className="text-base font-medium text-gray-900">
                  {new Date(report.created_at).toLocaleDateString('en-US', {
                    year: 'numeric',
                    month: 'long',
                    day: 'numeric',
                  })}
                </p>
              </div>
              <div>
                <p className="text-sm text-gray-500 mb-1">Status</p>
                <span
                  className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                    report.status === 'completed'
                      ? 'bg-green-100 text-green-800'
                      : report.status === 'processing'
                      ? 'bg-yellow-100 text-yellow-800'
                      : 'bg-gray-100 text-gray-800'
                  }`}
                >
                  {report.status.charAt(0).toUpperCase() + report.status.slice(1)}
                </span>
              </div>
              <div>
                <p className="text-sm text-gray-500 mb-1">Site URL</p>
                <p className="text-base font-medium text-gray-900 truncate">{report.site_url}</p>
              </div>
            </div>
          </div>

          {/* Module Sections */}
          <div className="space-y-8">
            {/* Module 1: Health & Trajectory */}
            <section id="module1" className="scroll-mt-20">
              {report.modules.module1 && (
                <Module1TrafficOverview data={report.modules.module1} />
              )}
              {!report.modules.module1 && (
                <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h2 className="text-2xl font-bold text-gray-900 mb-4">
                    01. Health & Trajectory
                  </h2>
                  <p className="text-gray-500">Module data not available</p>
                </div>
              )}
            </section>

            {/* Module 2: Page-Level Triage */}
            <section id="module2" className="scroll-mt-20">
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  02. Page-Level Triage
                </h2>
                {report.modules.module2 ? (
                  <div className="text-gray-600">
                    <p className="text-sm text-gray-500 mb-4">
                      Detailed page-level performance analysis and prioritization
                    </p>
                    <div className="bg-gray-50 rounded-lg p-4">
                      <p className="text-center text-gray-400">Module component coming soon</p>
                    </div>
                  </div>
                ) : (
                  <p className="text-gray-500">Module data not available</p>
                )}
              </div>
            </section>

            {/* Module 3: SERP Landscape Analysis */}
            <section id="module3" className="scroll-mt-20">
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  03. SERP Landscape Analysis
                </h2>
                {report.modules.module3 ? (
                  <div className="text-gray-600">
                    <p className="text-sm text-gray-500 mb-4">
                      SERP feature analysis, competitor mapping, and click share estimation
                    </p>
                    <div className="bg-gray-50 rounded-lg p-4">
                      <p className="text-center text-gray-400">Module component coming soon</p>
                    </div>
                  </div>
                ) : (
                  <p className="text-gray-500">Module data not available</p>
                )}
              </div>
            </section>

            {/* Module 4: Content Intelligence */}
            <section id="module4" className="scroll-mt-20">
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  04. Content Intelligence
                </h2>
                {report.modules.module4 ? (
                  <div className="text-gray-600">
                    <p className="text-sm text-gray-500 mb-4">
                      Cannibalization detection, striking distance opportunities, and content optimization
                    </p>
                    <div className="bg-gray-50 rounded-lg p-4">
                      <p className="text-center text-gray-400">Module component coming soon</p>
                    </div>
                  </div>
                ) : (
                  <p className="text-gray-500">Module data not available</p>
                )}
              </div>
            </section>

            {/* Module 5: The Gameplan */}
            <section id="module5" className="scroll-mt-20">
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">05. The Gameplan</h2>
                {report.modules.module5 ? (
                  <div className="text-gray-600">
                    <p className="text-sm text-gray-500 mb-4">
                      Prioritized action plan synthesized from all analysis modules
                    </p>
                    <div className="bg-gray-50 rounded-lg p-4">
                      <p className="text-center text-gray-400">Module component coming soon</p>
                    </div>
                  </div>
                ) : (
                  <p className="text-gray-500">Module data not available</p>
                )}
              </div>
            </section>

            {/* Module 6: Algorithm Update Impact */}
            <section id="module6" className="scroll-mt-20">
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  06. Algorithm Update Impact
                </h2>
                {report.modules.module6 ? (
                  <div className="text-gray-600">
                    <p className="text-sm text-gray-500 mb-4">
                      Historical algorithm update correlation and vulnerability assessment
                    </p>
                    <div className="bg-gray-50 rounded-lg p-4">
                      <p className="text-center text-gray-400">Module component coming soon</p>
                    </div>
                  </div>
                ) : (
                  <p className="text-gray-500">Module data not available</p>
                )}
              </div>
            </section>

            {/* Module 7: Query Intent Migration */}
            <section id="module7" className="scroll-mt-20">
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  07. Query Intent Migration
                </h2>
                {report.modules.module7 ? (
                  <div className="text-gray-600">
                    <p className="text-sm text-gray-500 mb-4">
                      Query evolution tracking and intent shift analysis
                    </p>
                    <div className="bg-gray-50 rounded-lg p-4">
                      <p className="text-center text-gray-400">Module component coming soon</p>
                    </div>
                  </div>
                ) : (
                  <p className="text-gray-500">Module data not available</p>
                )}
              </div>
            </section>

            {/* Module 8: Technical SEO Health */}
            <section id="module8" className="scroll-mt-20">
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  08. Technical SEO Health
                </h2>
                {report.modules.module8 ? (
                  <div className="text-gray-600">
                    <p className="text-sm text-gray-500 mb-4">
                      Crawl analysis, indexing status, and technical optimization opportunities
                    </p>
                    <div className="bg-gray-50 rounded-lg p-4">
                      <p className="text-center text-gray-400">Module component coming soon</p>
                    </div>
                  </div>
                ) : (
                  <p className="text-gray-500">Module data not available</p>
                )}
              </div>
            </section>

            {/* Module 9: Backlink Intelligence */}
            <section id="module9" className="scroll-mt-20">
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  09. Backlink Intelligence
                </h2>
                {report.modules.module9 ? (
                  <div className="text-gray-600">
                    <p className="text-sm text-gray-500 mb-4">
                      Backlink profile analysis and link building opportunities
                    </p>
                    <div className="bg-gray-50 rounded-lg p-4">
                      <p className="text-center text-gray-400">Module component coming soon</p>
                    </div>
                  </div>
                ) : (
                  <p className="text-gray-500">Module data not available</p>
                )}
              </div>
            </section>

            {/* Module 10: Competitive Intelligence */}
            <section id="module10" className="scroll-mt-20">
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  10. Competitive Intelligence
                </h2>
                {report.modules.module10 ? (
                  <div className="text-gray-600">
                    <p className="text-sm text-gray-500 mb-4">
                      Competitive positioning and market opportunity analysis
                    </p>
                    <div className="bg-gray-50 rounded-lg p-4">
                      <p className="text-center text-gray-400">Module component coming soon</p>
                    </div>
                  </div>
                ) : (
                  <p className="text-gray-500">Module data not available</p>
                )}
              </div>
            </section>

            {/* Module 11: Predictive Models */}
            <section id="module11" className="scroll-mt-20">
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  11. Predictive Models
                </h2>
                {report.modules.module11 ? (
                  <div className="text-gray-600">
                    <p className="text-sm text-gray-500 mb-4">
                      Machine learning-based traffic forecasting and scenario modeling
                    </p>
                    <div className="bg-gray-50 rounded-lg p-4">
                      <p className="text-center text-gray-400">Module component coming soon</p>
                    </div>
                  </div>
                ) : (
                  <p className="text-gray-500">Module data not available</p>
                )}
              </div>
            </section>

            {/* Module 12: Executive Summary */}
            <section id="module12" className="scroll-mt-20">
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  12. Executive Summary
                </h2>
                {report.modules.module12 ? (
                  <div className="text-gray-600">
                    <p className="text-sm text-gray-500 mb-4">
                      High-level findings and strategic recommendations
                    </p>
                    <div className="bg-gray-50 rounded-lg p-4">
                      <p className="text-center text-gray-400">Module component coming soon</p>
                    </div>
                  </div>
                ) : (
                  <p className="text-gray-500">Module data not available</p>
                )}
              </div>
            </section>
          </div>

          {/* Footer */}
          <footer className="mt-12 pt-8 border-t border-gray-200">
            <div className="text-center text-sm text-gray-500">
              <p>Search Intelligence Report</p>
              <p className="mt-1">
                Generated on{' '}
                {new Date(report.created_at).toLocaleDateString('en-US', {
                  year: 'numeric',
                  month: 'long',
                  day: 'numeric',
                  hour: '2-digit',
                  minute: '2-digit',
                })}
              </p>
            </div>
          </footer>
        </main>
      </div>
    </div>
  );
};

export default Report;