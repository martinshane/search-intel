import { Suspense } from 'react';
import { notFound } from 'next/navigation';
import Module1TrafficOverview from '@/components/modules/Module1TrafficOverview';

interface ReportPageProps {
  params: {
    id: string;
  };
}

interface ReportData {
  id: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  site_url: string;
  date_range: {
    start_date: string;
    end_date: string;
  };
  created_at: string;
  completed_at?: string;
  error_message?: string;
  modules: {
    module1_health_trajectory?: {
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
    };
    module2_page_triage?: any;
    module3_serp_landscape?: any;
    module4_content_intelligence?: any;
    module5_gameplan?: any;
    module6_algorithm_impacts?: any;
    module7_query_intent_migration?: any;
    module8_internal_link_analysis?: any;
    module9_technical_health?: any;
    module10_competitive_intelligence?: any;
    module11_search_demand_forecasting?: any;
    module12_revenue_impact?: any;
  };
}

async function getReport(id: string): Promise<ReportData | null> {
  const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
  
  try {
    const response = await fetch(`${apiUrl}/api/reports/${id}`, {
      cache: 'no-store',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      if (response.status === 404) {
        return null;
      }
      throw new Error(`Failed to fetch report: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Error fetching report:', error);
    throw error;
  }
}

function ReportMetadata({ report }: { report: ReportData }) {
  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
    });
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return 'text-green-600 bg-green-50 border-green-200';
      case 'processing':
        return 'text-blue-600 bg-blue-50 border-blue-200';
      case 'failed':
        return 'text-red-600 bg-red-50 border-red-200';
      default:
        return 'text-gray-600 bg-gray-50 border-gray-200';
    }
  };

  const getStatusText = (status: string) => {
    switch (status) {
      case 'completed':
        return 'Complete';
      case 'processing':
        return 'Processing...';
      case 'failed':
        return 'Failed';
      case 'pending':
        return 'Pending';
      default:
        return status;
    }
  };

  return (
    <div className="bg-white border-b border-gray-200">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <h1 className="text-3xl font-bold text-gray-900 mb-2">
              Search Intelligence Report
            </h1>
            <div className="flex flex-col space-y-2">
              <div className="flex items-center space-x-4 text-sm text-gray-600">
                <div className="flex items-center">
                  <svg
                    className="w-4 h-4 mr-1.5 text-gray-400"
                    fill="none"
                    stroke="currentColor"
                    viewBox="0 0 24 24"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"
                    />
                  </svg>
                  <span className="font-medium">{report.site_url}</span>
                </div>
                <div className="flex items-center">
                  <svg
                    className="w-4 h-4 mr-1.5 text-gray-400"
                    fill="none"
                    stroke="currentColor"
                    viewBox="0 0 24 24"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"
                    />
                  </svg>
                  <span>
                    {formatDate(report.date_range.start_date)} –{' '}
                    {formatDate(report.date_range.end_date)}
                  </span>
                </div>
              </div>
              <div className="flex items-center text-sm text-gray-500">
                <span>Generated on {formatDate(report.created_at)}</span>
                {report.completed_at && (
                  <span className="ml-4">
                    Completed on {formatDate(report.completed_at)}
                  </span>
                )}
              </div>
            </div>
          </div>
          <div>
            <span
              className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium border ${getStatusColor(
                report.status
              )}`}
            >
              {getStatusText(report.status)}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}

function ReportError({ message }: { message: string }) {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <div className="bg-red-50 border border-red-200 rounded-lg p-6">
        <div className="flex items-start">
          <svg
            className="w-6 h-6 text-red-600 mr-3 flex-shrink-0"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
            />
          </svg>
          <div>
            <h3 className="text-lg font-medium text-red-900 mb-1">
              Report Generation Failed
            </h3>
            <p className="text-sm text-red-700">{message}</p>
          </div>
        </div>
      </div>
    </div>
  );
}

function ReportProcessing() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
        <div className="flex items-start">
          <div className="flex-shrink-0">
            <svg
              className="animate-spin h-6 w-6 text-blue-600"
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
              />
              <path
                className="opacity-75"
                fill="currentColor"
                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
              />
            </svg>
          </div>
          <div className="ml-3">
            <h3 className="text-lg font-medium text-blue-900 mb-1">
              Generating Your Report
            </h3>
            <p className="text-sm text-blue-700 mb-4">
              This typically takes 2-5 minutes. We're analyzing your search data across
              multiple dimensions.
            </p>
            <div className="space-y-2 text-sm text-blue-600">
              <div className="flex items-center">
                <div className="w-2 h-2 bg-blue-400 rounded-full mr-2 animate-pulse" />
                <span>Fetching Google Search Console data...</span>
              </div>
              <div className="flex items-center">
                <div className="w-2 h-2 bg-blue-400 rounded-full mr-2 animate-pulse animation-delay-200" />
                <span>Pulling GA4 analytics...</span>
              </div>
              <div className="flex items-center">
                <div className="w-2 h-2 bg-blue-400 rounded-full mr-2 animate-pulse animation-delay-400" />
                <span>Analyzing SERP landscape...</span>
              </div>
            </div>
            <p className="text-xs text-blue-600 mt-4">
              You can safely close this page. We'll email you when your report is ready.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

function ReportContent({ report }: { report: ReportData }) {
  if (report.status === 'failed') {
    return <ReportError message={report.error_message || 'An unknown error occurred'} />;
  }

  if (report.status === 'processing' || report.status === 'pending') {
    return <ReportProcessing />;
  }

  const hasModule1 = report.modules?.module1_health_trajectory;

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Module 1: Health & Trajectory */}
      {hasModule1 && (
        <section className="mb-12">
          <Module1TrafficOverview data={report.modules.module1_health_trajectory!} />
        </section>
      )}

      {/* Placeholder for Module 2: Page-Level Triage */}
      <section className="mb-12">
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <h2 className="text-2xl font-bold text-gray-400 mb-2">
            Module 2: Page-Level Triage
          </h2>
          <p className="text-gray-500">Coming soon</p>
        </div>
      </section>

      {/* Placeholder for Module 3: SERP Landscape */}
      <section className="mb-12">
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <h2 className="text-2xl font-bold text-gray-400 mb-2">
            Module 3: SERP Landscape Analysis
          </h2>
          <p className="text-gray-500">Coming soon</p>
        </div>
      </section>

      {/* Placeholder for Module 4: Content Intelligence */}
      <section className="mb-12">
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <h2 className="text-2xl font-bold text-gray-400 mb-2">
            Module 4: Content Intelligence
          </h2>
          <p className="text-gray-500">Coming soon</p>
        </div>
      </section>

      {/* Placeholder for Module 5: The Gameplan */}
      <section className="mb-12">
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <h2 className="text-2xl font-bold text-gray-400 mb-2">
            Module 5: The Gameplan
          </h2>
          <p className="text-gray-500">Coming soon</p>
        </div>
      </section>

      {/* Placeholder for Module 6: Algorithm Update Impact */}
      <section className="mb-12">
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <h2 className="text-2xl font-bold text-gray-400 mb-2">
            Module 6: Algorithm Update Impact Analysis
          </h2>
          <p className="text-gray-500">Coming soon</p>
        </div>
      </section>

      {/* Placeholder for Module 7: Query Intent Migration */}
      <section className="mb-12">
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <h2 className="text-2xl font-bold text-gray-400 mb-2">
            Module 7: Query Intent Migration Tracking
          </h2>
          <p className="text-gray-500">Coming soon</p>
        </div>
      </section>

      {/* Placeholder for Module 8: Internal Link Analysis */}
      <section className="mb-12">
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <h2 className="text-2xl font-bold text-gray-400 mb-2">
            Module 8: Internal Link Graph Analysis
          </h2>
          <p className="text-gray-500">Coming soon</p>
        </div>
      </section>

      {/* Placeholder for Module 9: Technical Health */}
      <section className="mb-12">
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <h2 className="text-2xl font-bold text-gray-400 mb-2">
            Module 9: Technical Health Scoring
          </h2>
          <p className="text-gray-500">Coming soon</p>
        </div>
      </section>

      {/* Placeholder for Module 10: Competitive Intelligence */}
      <section className="mb-12">
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <h2 className="text-2xl font-bold text-gray-400 mb-2">
            Module 10: Competitive Intelligence
          </h2>
          <p className="text-gray-500">Coming soon</p>
        </div>
      </section>

      {/* Placeholder for Module 11: Search Demand Forecasting */}
      <section className="mb-12">
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <h2 className="text-2xl font-bold text-gray-400 mb-2">
            Module 11: Search Demand Forecasting
          </h2>
          <p className="text-gray-500">Coming soon</p>
        </div>
      </section>

      {/* Placeholder for Module 12: Revenue Impact */}
      <section className="mb-12">
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <h2 className="text-2xl font-bold text-gray-400 mb-2">
            Module 12: Revenue Impact Modeling
          </h2>
          <p className="text-gray-500">Coming soon</p>
        </div>
      </section>
    </div>
  );
}

function LoadingSkeleton() {
  return (
    <div className="animate-pulse">
      <div className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="h-8 bg-gray-200 rounded w-1/3 mb-4" />
          <div className="h-4 bg-gray-200 rounded w-1/2 mb-2" />
          <div className="h-4 bg-gray-200 rounded w-1/4" />
        </div>
      </div>
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="bg-gray-200 rounded-lg h-96 mb-8" />
        <div className="bg-gray-200 rounded-lg h-96" />
      </div>
    </div>
  );
}

export default async function ReportPage({ params }: ReportPageProps) {
  const report = await getReport(params.id);

  if (!report) {
    notFound();
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <ReportMetadata report={report} />
      <Suspense fallback={<LoadingSkeleton />}>
        <ReportContent report={report} />
      </Suspense>
    </div>
  );
}