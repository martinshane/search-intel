import { Suspense } from 'react'
import { notFound } from 'next/navigation'
import { ReportHeader } from '@/components/report/ReportHeader'
import { ReportSection } from '@/components/report/ReportSection'
import { Module1TrafficOverview } from '@/components/modules/Module1TrafficOverview'
import { LoadingSpinner } from '@/components/ui/LoadingSpinner'
import { ErrorBoundary } from '@/components/ui/ErrorBoundary'

interface PageProps {
  params: {
    id: string
  }
}

interface ReportData {
  id: string
  site_url: string
  status: 'pending' | 'processing' | 'completed' | 'failed'
  created_at: string
  completed_at?: string
  error_message?: string
  modules: {
    module1_health_trajectory?: Module1Data
    module2_page_triage?: any
    module3_serp_landscape?: any
    module4_content_intelligence?: any
    module5_gameplan?: any
    module6_algorithm_impacts?: any
    module7_intent_migration?: any
    module8_internal_link_equity?: any
    module9_technical_seo?: any
    module10_conversion_funnel?: any
    module11_competitive_intelligence?: any
    module12_predictive_model?: any
  }
}

interface Module1Data {
  overall_direction: string
  trend_slope_pct_per_month: number
  change_points: Array<{
    date: string
    magnitude: number
    direction: string
  }>
  seasonality: {
    best_day: string
    worst_day: string
    monthly_cycle: boolean
    cycle_description?: string
  }
  anomalies: Array<{
    date: string
    type: string
    magnitude: number
  }>
  forecast: {
    '30d': { clicks: number; ci_low: number; ci_high: number }
    '60d': { clicks: number; ci_low: number; ci_high: number }
    '90d': { clicks: number; ci_low: number; ci_high: number }
  }
  time_series_data?: Array<{
    date: string
    clicks: number
    impressions: number
    trend?: number
    seasonal?: number
    residual?: number
  }>
}

async function getReport(id: string): Promise<ReportData> {
  const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
  
  const res = await fetch(`${apiUrl}/api/reports/${id}`, {
    cache: 'no-store',
    headers: {
      'Content-Type': 'application/json',
    },
  })

  if (!res.ok) {
    if (res.status === 404) {
      notFound()
    }
    throw new Error(`Failed to fetch report: ${res.statusText}`)
  }

  return res.json()
}

function ReportContent({ report }: { report: ReportData }) {
  const hasModule1 = report.modules.module1_health_trajectory !== undefined
  const hasModule2 = report.modules.module2_page_triage !== undefined
  const hasModule3 = report.modules.module3_serp_landscape !== undefined
  const hasModule4 = report.modules.module4_content_intelligence !== undefined
  const hasModule5 = report.modules.module5_gameplan !== undefined
  const hasModule6 = report.modules.module6_algorithm_impacts !== undefined
  const hasModule7 = report.modules.module7_intent_migration !== undefined
  const hasModule8 = report.modules.module8_internal_link_equity !== undefined
  const hasModule9 = report.modules.module9_technical_seo !== undefined
  const hasModule10 = report.modules.module10_conversion_funnel !== undefined
  const hasModule11 = report.modules.module11_competitive_intelligence !== undefined
  const hasModule12 = report.modules.module12_predictive_model !== undefined

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <ReportHeader
          siteUrl={report.site_url}
          status={report.status}
          createdAt={report.created_at}
          completedAt={report.completed_at}
        />

        {report.status === 'failed' && (
          <div className="mt-6 bg-red-50 border border-red-200 rounded-lg p-6">
            <div className="flex items-start">
              <div className="flex-shrink-0">
                <svg
                  className="h-6 w-6 text-red-400"
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
              </div>
              <div className="ml-3">
                <h3 className="text-sm font-medium text-red-800">
                  Report generation failed
                </h3>
                {report.error_message && (
                  <div className="mt-2 text-sm text-red-700">
                    <p>{report.error_message}</p>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {report.status === 'pending' && (
          <div className="mt-6 bg-blue-50 border border-blue-200 rounded-lg p-6">
            <div className="flex items-center">
              <LoadingSpinner size="sm" />
              <p className="ml-3 text-sm text-blue-700">
                Your report is queued for generation. This page will automatically update.
              </p>
            </div>
          </div>
        )}

        {report.status === 'processing' && (
          <div className="mt-6 bg-blue-50 border border-blue-200 rounded-lg p-6">
            <div className="flex items-center">
              <LoadingSpinner size="sm" />
              <div className="ml-3">
                <p className="text-sm font-medium text-blue-800">
                  Generating your report...
                </p>
                <p className="text-sm text-blue-700 mt-1">
                  This typically takes 2-5 minutes. The page will update automatically as modules complete.
                </p>
              </div>
            </div>
          </div>
        )}

        <div className="mt-8 space-y-8">
          <ErrorBoundary
            fallback={
              <div className="bg-red-50 border border-red-200 rounded-lg p-6">
                <p className="text-sm text-red-800">
                  Failed to load Health & Trajectory module. Please refresh the page.
                </p>
              </div>
            }
          >
            <ReportSection
              title="Module 1: Health & Trajectory"
              description="Statistical decomposition of traffic patterns, change point detection, and predictive forecasting"
              moduleNumber={1}
              isComplete={hasModule1}
              isProcessing={report.status === 'processing' && !hasModule1}
            >
              {hasModule1 && (
                <Module1TrafficOverview
                  data={report.modules.module1_health_trajectory!}
                />
              )}
            </ReportSection>
          </ErrorBoundary>

          <ErrorBoundary
            fallback={
              <div className="bg-red-50 border border-red-200 rounded-lg p-6">
                <p className="text-sm text-red-800">
                  Failed to load Page-Level Triage module. Please refresh the page.
                </p>
              </div>
            }
          >
            <ReportSection
              title="Module 2: Page-Level Triage"
              description="Per-page trend analysis, CTR anomaly detection, and engagement cross-reference"
              moduleNumber={2}
              isComplete={hasModule2}
              isProcessing={report.status === 'processing' && hasModule1 && !hasModule2}
            >
              {hasModule2 && (
                <div className="p-6 bg-white rounded-lg border border-gray-200">
                  <pre className="text-xs overflow-auto">
                    {JSON.stringify(report.modules.module2_page_triage, null, 2)}
                  </pre>
                </div>
              )}
            </ReportSection>
          </ErrorBoundary>

          <ErrorBoundary
            fallback={
              <div className="bg-red-50 border border-red-200 rounded-lg p-6">
                <p className="text-sm text-red-800">
                  Failed to load SERP Landscape module. Please refresh the page.
                </p>
              </div>
            }
          >
            <ReportSection
              title="Module 3: SERP Landscape Analysis"
              description="SERP feature displacement, competitor mapping, and intent classification"
              moduleNumber={3}
              isComplete={hasModule3}
              isProcessing={report.status === 'processing' && hasModule2 && !hasModule3}
            >
              {hasModule3 && (
                <div className="p-6 bg-white rounded-lg border border-gray-200">
                  <pre className="text-xs overflow-auto">
                    {JSON.stringify(report.modules.module3_serp_landscape, null, 2)}
                  </pre>
                </div>
              )}
            </ReportSection>
          </ErrorBoundary>

          <ErrorBoundary
            fallback={
              <div className="bg-red-50 border border-red-200 rounded-lg p-6">
                <p className="text-sm text-red-800">
                  Failed to load Content Intelligence module. Please refresh the page.
                </p>
              </div>
            }
          >
            <ReportSection
              title="Module 4: Content Intelligence"
              description="Cannibalization detection, striking distance opportunities, and content gap analysis"
              moduleNumber={4}
              isComplete={hasModule4}
              isProcessing={report.status === 'processing' && hasModule3 && !hasModule4}
            >
              {hasModule4 && (
                <div className="p-6 bg-white rounded-lg border border-gray-200">
                  <pre className="text-xs overflow-auto">
                    {JSON.stringify(report.modules.module4_content_intelligence, null, 2)}
                  </pre>
                </div>
              )}
            </ReportSection>
          </ErrorBoundary>

          <ErrorBoundary
            fallback={
              <div className="bg-red-50 border border-red-200 rounded-lg p-6">
                <p className="text-sm text-red-800">
                  Failed to load Gameplan module. Please refresh the page.
                </p>
              </div>
            }
          >
            <ReportSection
              title="Module 5: The Gameplan"
              description="Prioritized action list synthesizing all prior analysis into concrete next steps"
              moduleNumber={5}
              isComplete={hasModule5}
              isProcessing={report.status === 'processing' && hasModule4 && !hasModule5}
            >
              {hasModule5 && (
                <div className="p-6 bg-white rounded-lg border border-gray-200">
                  <pre className="text-xs overflow-auto">
                    {JSON.stringify(report.modules.module5_gameplan, null, 2)}
                  </pre>
                </div>
              )}
            </ReportSection>
          </ErrorBoundary>

          <ErrorBoundary
            fallback={
              <div className="bg-red-50 border border-red-200 rounded-lg p-6">
                <p className="text-sm text-red-800">
                  Failed to load Algorithm Update Impact module. Please refresh the page.
                </p>
              </div>
            }
          >
            <ReportSection
              title="Module 6: Algorithm Update Impact Analysis"
              description="Correlation of traffic changes with known algorithm updates and vulnerability assessment"
              moduleNumber={6}
              isComplete={hasModule6}
              isProcessing={report.status === 'processing' && hasModule5 && !hasModule6}
            >
              {hasModule6 && (
                <div className="p-6 bg-white rounded-lg border border-gray-200">
                  <pre className="text-xs overflow-auto">
                    {JSON.stringify(report.modules.module6_algorithm_impacts, null, 2)}
                  </pre>
                </div>
              )}
            </ReportSection>
          </ErrorBoundary>

          <ErrorBoundary
            fallback={
              <div className="bg-red-50 border border-red-200 rounded-lg p-6">
                <p className="text-sm text-red-800">
                  Failed to load Query Intent Migration module. Please refresh the page.
                </p>
              </div>
            }
          >
            <ReportSection
              title="Module 7: Query Intent Migration Tracking"
              description="Detection of search intent evolution and SERP composition changes over time"
              moduleNumber={7}
              isComplete={hasModule7}
              isProcessing={report.status === 'processing' && hasModule6 && !hasModule7}
            >
              {hasModule7 && (
                <div className="p-6 bg-white rounded-lg border border-gray-200">
                  <pre className="text-xs overflow-auto">
                    {JSON.stringify(report.modules.module7_intent_migration, null, 2)}
                  </pre>
                </div>
              )}
            </ReportSection>
          </ErrorBoundary>

          <ErrorBoundary
            fallback={
              <div className="bg-red-50 border border-red-200 rounded-lg p-6">
                <p className="text-sm text-red-800">
                  Failed to load Internal Link Equity module. Please refresh the page.
                </p>
              </div>
            }
          >
            <ReportSection
              title="Module 8: Internal Link Equity Flow"
              description="PageRank-style analysis of internal link architecture and authority distribution"
              moduleNumber={8}
              isComplete={hasModule8}
              isProcessing={report.status === 'processing' && hasModule7 && !hasModule8}
            >
              {hasModule8 && (
                <div className="p-6 bg-white rounded-lg border border-gray-200">
                  <pre className="text-xs overflow-auto">
                    {JSON.stringify(report.modules.module8_internal_link_equity, null, 2)}
                  </pre>
                </div>
              )}
            </ReportSection>
          </ErrorBoundary>

          <ErrorBoundary
            fallback={
              <div className="bg-red-50 border border-red-200 rounded-lg p-6">
                <p className="text-sm text-red-800">
                  Failed to load Technical SEO module. Please refresh the page.
                </p>
              </div>
            }
          >
            <ReportSection
              title="Module 9: Technical SEO Health"
              description="Crawl-based technical audit covering indexability, performance, and schema implementation"
              moduleNumber={9}
              isComplete={hasModule9}
              isProcessing={report.status === 'processing' && hasModule8 && !hasModule9}
            >
              {hasModule9 && (
                <div className="p-6 bg-white rounded-lg border border-gray-200">
                  <pre className="text-xs overflow-auto">
                    {JSON.stringify(report.modules.module9_technical_seo, null, 2)}
                  </pre>
                </div>
              )}
            </ReportSection>
          </ErrorBoundary>

          <ErrorBoundary
            fallback={
              <div className="bg-red-50 border border-red-200 rounded-lg p-6">
                <p className="text-sm text-red-800">
                  Failed to load Conversion Funnel module. Please refresh the page.
                </p>
              </div>
            }
          >
            <ReportSection
              title="Module 10: Conversion Funnel Intelligence"
              description="Multi-touch attribution and conversion path analysis from search to goal completion"
              moduleNumber={10}
              isComplete={hasModule10}
              isProcessing={report.status === 'processing' && hasModule9 && !hasModule10}
            >
              {hasModule10 && (
                <div className="p-6 bg-white rounded-lg border border-gray-200">
                  <pre className="text-xs overflow-auto">
                    {JSON.stringify(report.modules.module10_conversion_funnel, null, 2)}
                  </pre>
                </div>
              )}
            </ReportSection>
          </ErrorBoundary>

          <ErrorBoundary
            fallback={
              <div className="bg-red-50 border border-red-200 rounded-lg p-6">
                <p className="text-sm text-red-800">
                  Failed to load Competitive Intelligence module. Please refresh the page.
                </p>
              </div>
            }
          >
            <ReportSection
              title="Module 11: Competitive Intelligence"
              description="Deep competitor analysis including content gap identification and ranking overlap"
              moduleNumber={11}
              isComplete={hasModule11}
              isProcessing={report.status === 'processing' && hasModule10 && !hasModule11}
            >
              {hasModule11 && (
                <div className="p-6 bg-white rounded-lg border border-gray-200">
                  <pre className="text-xs overflow-auto">
                    {JSON.stringify(report.modules.module11_competitive_intelligence, null, 2)}
                  </pre>
                </div>
              )}
            </ReportSection>
          </ErrorBoundary>

          <ErrorBoundary
            fallback={
              <div className="bg-red-50 border border-red-200 rounded-lg p-6">
                <p className="text-sm text-red-800">
                  Failed to load Predictive Model module. Please refresh the page.
                </p>
              </div>
            }
          >
            <ReportSection
              title="Module 12: Predictive Traffic Model"
              description="Machine learning model for forecasting traffic impact of proposed changes"
              moduleNumber={12}
              isComplete={hasModule12}
              isProcessing={report.status === 'processing' && hasModule11 && !hasModule12}
            >
              {hasModule12 && (
                <div className="p-6 bg-white rounded-lg border border-gray-200">
                  <pre className="text-xs overflow-auto">
                    {JSON.stringify(report.modules.module12_predictive_model, null, 2)}
                  </pre>
                </div>
              )}
            </ReportSection>
          </ErrorBoundary>
        </div>

        {report.status === 'completed' && (
          <div className="mt-12 bg-green-50 border border-green-200 rounded-lg p-6">
            <div className="flex items-start">
              <div className="flex-shrink-0">
                <svg
                  className="h-6 w-6 text-green-400"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
                  />
                </svg>
              </div>
              <div className="ml-3">
                <h3 className="text-sm font-medium text-green-800">
                  Report completed
                </h3>
                <div className="mt-2 text-sm text-green-700">
                  <p>
                    Your comprehensive Search Intelligence Report has been generated successfully.
                    All {Object.keys(report.modules).length} analysis modules are complete.
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default async function ReportPage({ params }: PageProps) {
  const report = await getReport(params.id)

  return (
    <Suspense
      fallback={
        <div className="min-h-screen bg-gray-50 flex items-center justify-center">
          <div className="text-center">
            <LoadingSpinner size="lg" />
            <p className="mt-4 text-sm text-gray-600">Loading report...</p>
          </div>
        </div>
      }
    >
      <ReportContent report={report} />
    </Suspense>
  )
}

export const revalidate = 30

export async function generateMetadata({ params }: PageProps) {
  try {
    const report = await getReport(params.id)
    return {
      title: `Search Intelligence Report - ${report.site_url}`,
      description: `Comprehensive search intelligence analysis for ${report.site_url}`,
    }
  } catch (error) {
    return {
      title: 'Search Intelligence Report',
      description: 'Comprehensive search intelligence analysis',
    }
  }
}
