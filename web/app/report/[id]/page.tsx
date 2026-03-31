import { Metadata } from 'next';
import { notFound } from 'next/navigation';
import Link from 'next/link';

import ModuleOne from '@/components/ModuleOne';

interface Report {
  id: string;
  site_url: string;
  created_at: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  error_message?: string;
  report_data?: {
    module_1?: any;
    module_2?: any;
    module_3?: any;
    module_4?: any;
    module_5?: any;
    module_6?: any;
    module_7?: any;
    module_8?: any;
    module_9?: any;
    module_10?: any;
    module_11?: any;
    module_12?: any;
  };
}

async function getReport(id: string): Promise<Report | null> {
  try {
    const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/reports/${id}`, {
      cache: 'no-store',
    });

    if (!res.ok) {
      if (res.status === 404) {
        return null;
      }
      throw new Error('Failed to fetch report');
    }

    return res.json();
  } catch (error) {
    console.error('Error fetching report:', error);
    throw error;
  }
}

export async function generateMetadata({
  params,
}: {
  params: { id: string };
}): Promise<Metadata> {
  const report = await getReport(params.id);

  if (!report) {
    return {
      title: 'Report Not Found',
    };
  }

  return {
    title: `Search Intelligence Report - ${report.site_url}`,
    description: `Comprehensive search intelligence analysis for ${report.site_url}`,
  };
}

export default async function ReportPage({ params }: { params: { id: string } }) {
  const report = await getReport(params.id);

  if (!report) {
    notFound();
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <Link
                href="/"
                className="text-sm text-gray-500 hover:text-gray-700 mb-2 inline-block"
              >
                ← Back to Dashboard
              </Link>
              <h1 className="text-3xl font-bold text-gray-900">Search Intelligence Report</h1>
              <p className="mt-2 text-lg text-gray-600">{report.site_url}</p>
            </div>
            <div className="text-right">
              <div className="text-sm text-gray-500">Generated</div>
              <div className="text-sm font-medium text-gray-900">
                {new Date(report.created_at).toLocaleDateString('en-US', {
                  year: 'numeric',
                  month: 'long',
                  day: 'numeric',
                  hour: '2-digit',
                  minute: '2-digit',
                })}
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Status Banner */}
      {report.status === 'pending' && (
        <div className="bg-yellow-50 border-b border-yellow-200">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <svg
                  className="h-5 w-5 text-yellow-400"
                  xmlns="http://www.w3.org/2000/svg"
                  viewBox="0 0 20 20"
                  fill="currentColor"
                >
                  <path
                    fillRule="evenodd"
                    d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-12a1 1 0 10-2 0v4a1 1 0 00.293.707l2.828 2.829a1 1 0 101.415-1.415L11 9.586V6z"
                    clipRule="evenodd"
                  />
                </svg>
              </div>
              <div className="ml-3">
                <p className="text-sm text-yellow-700">
                  Report generation is queued. This usually takes 2-5 minutes.
                </p>
              </div>
            </div>
          </div>
        </div>
      )}

      {report.status === 'processing' && (
        <div className="bg-blue-50 border-b border-blue-200">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <svg
                  className="animate-spin h-5 w-5 text-blue-400"
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  viewBox="0 0 24 24"
                >
                  <circle
                    className="opacity-25"
                    cx="12"
                    cy="12"
                    r="10"
                    stroke="currentColor"
                    strokeWidth="4"
                  ></circle>
                  <path
                    className="opacity-75"
                    fill="currentColor"
                    d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                  ></path>
                </svg>
              </div>
              <div className="ml-3">
                <p className="text-sm text-blue-700">
                  Report is being generated. Analyzing your search data...
                </p>
              </div>
            </div>
          </div>
        </div>
      )}

      {report.status === 'failed' && (
        <div className="bg-red-50 border-b border-red-200">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <svg
                  className="h-5 w-5 text-red-400"
                  xmlns="http://www.w3.org/2000/svg"
                  viewBox="0 0 20 20"
                  fill="currentColor"
                >
                  <path
                    fillRule="evenodd"
                    d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
                    clipRule="evenodd"
                  />
                </svg>
              </div>
              <div className="ml-3">
                <p className="text-sm text-red-700">
                  Report generation failed: {report.error_message || 'Unknown error'}
                </p>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {report.status === 'completed' && report.report_data ? (
          <div className="space-y-8">
            {/* Executive Summary */}
            <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
              <h2 className="text-2xl font-bold text-gray-900 mb-4">Executive Summary</h2>
              <p className="text-gray-600 leading-relaxed">
                This comprehensive search intelligence report analyzes 16 months of search
                performance data across 12 integrated modules. Each section builds
                progressively from raw data through statistical analysis, cross-dataset
                correlation, and predictive modeling to deliver prioritized, actionable
                recommendations.
              </p>
            </section>

            {/* Analysis Modules */}
            <div className="space-y-6">
              {/* Module 1: Health & Trajectory */}
              {report.report_data.module_1 && (
                <ModuleOne data={report.report_data.module_1} />
              )}

              {/* Module 2: Page-Level Triage */}
              {report.report_data.module_2 && (
                <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h2 className="text-2xl font-bold text-gray-900 mb-4">
                    Module 2: Page-Level Triage
                  </h2>
                  <div className="text-gray-600">
                    <pre className="whitespace-pre-wrap text-sm">
                      {JSON.stringify(report.report_data.module_2, null, 2)}
                    </pre>
                  </div>
                </section>
              )}

              {/* Module 3: SERP Landscape Analysis */}
              {report.report_data.module_3 && (
                <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h2 className="text-2xl font-bold text-gray-900 mb-4">
                    Module 3: SERP Landscape Analysis
                  </h2>
                  <div className="text-gray-600">
                    <pre className="whitespace-pre-wrap text-sm">
                      {JSON.stringify(report.report_data.module_3, null, 2)}
                    </pre>
                  </div>
                </section>
              )}

              {/* Module 4: Content Intelligence */}
              {report.report_data.module_4 && (
                <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h2 className="text-2xl font-bold text-gray-900 mb-4">
                    Module 4: Content Intelligence
                  </h2>
                  <div className="text-gray-600">
                    <pre className="whitespace-pre-wrap text-sm">
                      {JSON.stringify(report.report_data.module_4, null, 2)}
                    </pre>
                  </div>
                </section>
              )}

              {/* Module 5: The Gameplan */}
              {report.report_data.module_5 && (
                <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h2 className="text-2xl font-bold text-gray-900 mb-4">
                    Module 5: The Gameplan
                  </h2>
                  <div className="text-gray-600">
                    <pre className="whitespace-pre-wrap text-sm">
                      {JSON.stringify(report.report_data.module_5, null, 2)}
                    </pre>
                  </div>
                </section>
              )}

              {/* Module 6: Algorithm Update Impact */}
              {report.report_data.module_6 && (
                <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h2 className="text-2xl font-bold text-gray-900 mb-4">
                    Module 6: Algorithm Update Impact Analysis
                  </h2>
                  <div className="text-gray-600">
                    <pre className="whitespace-pre-wrap text-sm">
                      {JSON.stringify(report.report_data.module_6, null, 2)}
                    </pre>
                  </div>
                </section>
              )}

              {/* Module 7: Query Intent Migration */}
              {report.report_data.module_7 && (
                <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h2 className="text-2xl font-bold text-gray-900 mb-4">
                    Module 7: Query Intent Migration Tracking
                  </h2>
                  <div className="text-gray-600">
                    <pre className="whitespace-pre-wrap text-sm">
                      {JSON.stringify(report.report_data.module_7, null, 2)}
                    </pre>
                  </div>
                </section>
              )}

              {/* Module 8: Internal Link Architecture */}
              {report.report_data.module_8 && (
                <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h2 className="text-2xl font-bold text-gray-900 mb-4">
                    Module 8: Internal Link Architecture
                  </h2>
                  <div className="text-gray-600">
                    <pre className="whitespace-pre-wrap text-sm">
                      {JSON.stringify(report.report_data.module_8, null, 2)}
                    </pre>
                  </div>
                </section>
              )}

              {/* Module 9: Seasonal Intelligence */}
              {report.report_data.module_9 && (
                <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h2 className="text-2xl font-bold text-gray-900 mb-4">
                    Module 9: Seasonal Intelligence
                  </h2>
                  <div className="text-gray-600">
                    <pre className="whitespace-pre-wrap text-sm">
                      {JSON.stringify(report.report_data.module_9, null, 2)}
                    </pre>
                  </div>
                </section>
              )}

              {/* Module 10: Technical SEO Health */}
              {report.report_data.module_10 && (
                <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h2 className="text-2xl font-bold text-gray-900 mb-4">
                    Module 10: Technical SEO Health
                  </h2>
                  <div className="text-gray-600">
                    <pre className="whitespace-pre-wrap text-sm">
                      {JSON.stringify(report.report_data.module_10, null, 2)}
                    </pre>
                  </div>
                </section>
              )}

              {/* Module 11: Conversion Funnel Attribution */}
              {report.report_data.module_11 && (
                <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h2 className="text-2xl font-bold text-gray-900 mb-4">
                    Module 11: Conversion Funnel Attribution
                  </h2>
                  <div className="text-gray-600">
                    <pre className="whitespace-pre-wrap text-sm">
                      {JSON.stringify(report.report_data.module_11, null, 2)}
                    </pre>
                  </div>
                </section>
              )}

              {/* Module 12: Predictive Scenario Modeling */}
              {report.report_data.module_12 && (
                <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h2 className="text-2xl font-bold text-gray-900 mb-4">
                    Module 12: Predictive Scenario Modeling
                  </h2>
                  <div className="text-gray-600">
                    <pre className="whitespace-pre-wrap text-sm">
                      {JSON.stringify(report.report_data.module_12, null, 2)}
                    </pre>
                  </div>
                </section>
              )}
            </div>

            {/* Footer CTA */}
            <section className="bg-gradient-to-r from-blue-600 to-blue-700 rounded-lg shadow-sm p-8 text-center">
              <h2 className="text-2xl font-bold text-white mb-4">
                Ready to Implement These Recommendations?
              </h2>
              <p className="text-blue-100 mb-6 max-w-2xl mx-auto">
                This report provides a comprehensive roadmap for improving your search
                performance. Need help executing these strategies? Let's talk about a custom
                engagement.
              </p>
              <button className="bg-white text-blue-600 font-semibold px-6 py-3 rounded-lg hover:bg-blue-50 transition-colors">
                Schedule a Consultation
              </button>
            </section>
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-12 text-center">
            <div className="max-w-md mx-auto">
              {report.status === 'pending' && (
                <>
                  <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-yellow-100 mb-4">
                    <svg
                      className="w-8 h-8 text-yellow-600"
                      fill="none"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth="2"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                    >
                      <path d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                    </svg>
                  </div>
                  <h3 className="text-lg font-semibold text-gray-900 mb-2">
                    Report Generation Queued
                  </h3>
                  <p className="text-gray-600">
                    Your report is waiting to be processed. This page will automatically update
                    when analysis begins.
                  </p>
                </>
              )}

              {report.status === 'processing' && (
                <>
                  <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-blue-100 mb-4">
                    <svg
                      className="animate-spin w-8 h-8 text-blue-600"
                      xmlns="http://www.w3.org/2000/svg"
                      fill="none"
                      viewBox="0 0 24 24"
                    >
                      <circle
                        className="opacity-25"
                        cx="12"
                        cy="12"
                        r="10"
                        stroke="currentColor"
                        strokeWidth="4"
                      ></circle>
                      <path
                        className="opacity-75"
                        fill="currentColor"
                        d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                      ></path>
                    </svg>
                  </div>
                  <h3 className="text-lg font-semibold text-gray-900 mb-2">
                    Analyzing Your Data
                  </h3>
                  <p className="text-gray-600">
                    We're pulling data from Google Search Console and running statistical
                    analysis. This typically takes 2-5 minutes.
                  </p>
                </>
              )}

              {report.status === 'failed' && (
                <>
                  <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-red-100 mb-4">
                    <svg
                      className="w-8 h-8 text-red-600"
                      fill="none"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth="2"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                    >
                      <path d="M6 18L18 6M6 6l12 12"></path>
                    </svg>
                  </div>
                  <h3 className="text-lg font-semibold text-gray-900 mb-2">
                    Generation Failed
                  </h3>
                  <p className="text-gray-600 mb-4">
                    {report.error_message ||
                      'An error occurred while generating your report. Please try again.'}
                  </p>
                  <Link
                    href="/"
                    className="inline-block bg-blue-600 text-white font-semibold px-6 py-2 rounded-lg hover:bg-blue-700 transition-colors"
                  >
                    Return to Dashboard
                  </Link>
                </>
              )}
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
