import { Metadata } from 'next';
import { notFound } from 'next/navigation';
import Link from 'next/link';
import { createClient } from '@supabase/supabase-js';

import Module1TechnicalHealth from '@/components/Module1TechnicalHealth';

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
);

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
    const { data, error } = await supabase
      .from('reports')
      .select('*')
      .eq('id', id)
      .single();

    if (error) {
      if (error.code === 'PGRST116') {
        return null;
      }
      throw error;
    }

    return data;
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
                  className="h-5 w-5 text-yellow-400 animate-spin"
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
                <p className="text-sm text-yellow-800">
                  Report generation queued. This page will update automatically.
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
                  className="h-5 w-5 text-blue-400 animate-spin"
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
                <p className="text-sm text-blue-800">
                  Analyzing your data... This typically takes 2-5 minutes. This page will update
                  automatically.
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
                <p className="text-sm text-red-800">
                  Report generation failed: {report.error_message || 'Unknown error occurred'}
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
            {/* Module 1: Technical Health & Trajectory */}
            {report.report_data.module_1 && (
              <Module1TechnicalHealth data={report.report_data.module_1} />
            )}

            {/* Module 2: Page-Level Triage */}
            {report.report_data.module_2 && (
              <section className="bg-white rounded-lg shadow-sm p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  Module 2: Page-Level Triage
                </h2>
                <div className="text-gray-600">
                  <pre className="bg-gray-50 p-4 rounded overflow-auto text-xs">
                    {JSON.stringify(report.report_data.module_2, null, 2)}
                  </pre>
                </div>
              </section>
            )}

            {/* Module 3: SERP Landscape Analysis */}
            {report.report_data.module_3 && (
              <section className="bg-white rounded-lg shadow-sm p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  Module 3: SERP Landscape Analysis
                </h2>
                <div className="text-gray-600">
                  <pre className="bg-gray-50 p-4 rounded overflow-auto text-xs">
                    {JSON.stringify(report.report_data.module_3, null, 2)}
                  </pre>
                </div>
              </section>
            )}

            {/* Module 4: Content Intelligence */}
            {report.report_data.module_4 && (
              <section className="bg-white rounded-lg shadow-sm p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  Module 4: Content Intelligence
                </h2>
                <div className="text-gray-600">
                  <pre className="bg-gray-50 p-4 rounded overflow-auto text-xs">
                    {JSON.stringify(report.report_data.module_4, null, 2)}
                  </pre>
                </div>
              </section>
            )}

            {/* Module 5: The Gameplan */}
            {report.report_data.module_5 && (
              <section className="bg-white rounded-lg shadow-sm p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">Module 5: The Gameplan</h2>
                <div className="text-gray-600">
                  <pre className="bg-gray-50 p-4 rounded overflow-auto text-xs">
                    {JSON.stringify(report.report_data.module_5, null, 2)}
                  </pre>
                </div>
              </section>
            )}

            {/* Module 6: Algorithm Update Impact Analysis */}
            {report.report_data.module_6 && (
              <section className="bg-white rounded-lg shadow-sm p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  Module 6: Algorithm Update Impact Analysis
                </h2>
                <div className="text-gray-600">
                  <pre className="bg-gray-50 p-4 rounded overflow-auto text-xs">
                    {JSON.stringify(report.report_data.module_6, null, 2)}
                  </pre>
                </div>
              </section>
            )}

            {/* Module 7: Query Intent Migration Tracking */}
            {report.report_data.module_7 && (
              <section className="bg-white rounded-lg shadow-sm p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  Module 7: Query Intent Migration Tracking
                </h2>
                <div className="text-gray-600">
                  <pre className="bg-gray-50 p-4 rounded overflow-auto text-xs">
                    {JSON.stringify(report.report_data.module_7, null, 2)}
                  </pre>
                </div>
              </section>
            )}

            {/* Module 8: Internal Link Authority Flow */}
            {report.report_data.module_8 && (
              <section className="bg-white rounded-lg shadow-sm p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  Module 8: Internal Link Authority Flow
                </h2>
                <div className="text-gray-600">
                  <pre className="bg-gray-50 p-4 rounded overflow-auto text-xs">
                    {JSON.stringify(report.report_data.module_8, null, 2)}
                  </pre>
                </div>
              </section>
            )}

            {/* Module 9: Conversion Funnel Intelligence */}
            {report.report_data.module_9 && (
              <section className="bg-white rounded-lg shadow-sm p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  Module 9: Conversion Funnel Intelligence
                </h2>
                <div className="text-gray-600">
                  <pre className="bg-gray-50 p-4 rounded overflow-auto text-xs">
                    {JSON.stringify(report.report_data.module_9, null, 2)}
                  </pre>
                </div>
              </section>
            )}

            {/* Module 10: Competitive Velocity Analysis */}
            {report.report_data.module_10 && (
              <section className="bg-white rounded-lg shadow-sm p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  Module 10: Competitive Velocity Analysis
                </h2>
                <div className="text-gray-600">
                  <pre className="bg-gray-50 p-4 rounded overflow-auto text-xs">
                    {JSON.stringify(report.report_data.module_10, null, 2)}
                  </pre>
                </div>
              </section>
            )}

            {/* Module 11: Predictive Traffic Modeling */}
            {report.report_data.module_11 && (
              <section className="bg-white rounded-lg shadow-sm p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  Module 11: Predictive Traffic Modeling
                </h2>
                <div className="text-gray-600">
                  <pre className="bg-gray-50 p-4 rounded overflow-auto text-xs">
                    {JSON.stringify(report.report_data.module_11, null, 2)}
                  </pre>
                </div>
              </section>
            )}

            {/* Module 12: Executive Summary & ROI Calculator */}
            {report.report_data.module_12 && (
              <section className="bg-white rounded-lg shadow-sm p-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-4">
                  Module 12: Executive Summary & ROI Calculator
                </h2>
                <div className="text-gray-600">
                  <pre className="bg-gray-50 p-4 rounded overflow-auto text-xs">
                    {JSON.stringify(report.report_data.module_12, null, 2)}
                  </pre>
                </div>
              </section>
            )}
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow-sm p-12 text-center">
            <svg
              className="mx-auto h-12 w-12 text-gray-400"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
              />
            </svg>
            <h3 className="mt-2 text-sm font-medium text-gray-900">No report data available</h3>
            <p className="mt-1 text-sm text-gray-500">
              {report.status === 'pending'
                ? 'Your report is queued for generation.'
                : report.status === 'processing'
                ? 'Your report is being generated.'
                : 'Report generation has not completed successfully.'}
            </p>
          </div>
        )}
      </main>
    </div>
  );
}
