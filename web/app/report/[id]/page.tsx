import { Suspense } from 'react';
import { redirect } from 'next/navigation';
import { createServerComponentClient } from '@supabase/auth-helpers-nextjs';
import { cookies } from 'next/headers';
import Link from 'next/link';
import { ArrowLeft, Download, ExternalLink } from 'lucide-react';
import Module1TrafficOverview from '@/components/modules/Module1TrafficOverview';

interface Report {
  id: string;
  site_domain: string;
  status: string;
  created_at: string;
  completed_at: string | null;
  error_message: string | null;
  results: any;
}

async function getReport(reportId: string): Promise<Report | null> {
  const supabase = createServerComponentClient({ cookies });

  const { data: report, error } = await supabase
    .from('reports')
    .select('*')
    .eq('id', reportId)
    .single();

  if (error || !report) {
    return null;
  }

  return report;
}

async function checkAuth() {
  const supabase = createServerComponentClient({ cookies });
  
  const {
    data: { session },
  } = await supabase.auth.getSession();

  if (!session) {
    redirect('/auth/signin');
  }

  return session;
}

function ReportHeader({ report }: { report: Report }) {
  return (
    <div className="border-b border-gray-200 bg-white px-6 py-6">
      <div className="max-w-7xl mx-auto">
        <div className="flex items-center justify-between mb-4">
          <Link
            href="/dashboard"
            className="flex items-center gap-2 text-sm font-medium text-gray-600 hover:text-gray-900 transition-colors"
          >
            <ArrowLeft className="h-4 w-4" />
            Back to Dashboard
          </Link>
          <div className="flex items-center gap-3">
            <button
              className="inline-flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
              onClick={() => window.print()}
            >
              <Download className="h-4 w-4" />
              Export PDF
            </button>
            <a
              href={`https://${report.site_domain}`}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
            >
              <ExternalLink className="h-4 w-4" />
              View Site
            </a>
          </div>
        </div>
        
        <div>
          <h1 className="text-3xl font-bold text-gray-900">
            Search Intelligence Report
          </h1>
          <div className="mt-2 flex items-center gap-4 text-sm text-gray-600">
            <span className="font-medium">{report.site_domain}</span>
            <span>•</span>
            <span>
              Generated {new Date(report.created_at).toLocaleDateString('en-US', {
                month: 'long',
                day: 'numeric',
                year: 'numeric',
              })}
            </span>
            {report.completed_at && (
              <>
                <span>•</span>
                <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                  <span className="h-1.5 w-1.5 rounded-full bg-green-600" />
                  Complete
                </span>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function ReportStatusBanner({ status, error }: { status: string; error: string | null }) {
  if (status === 'completed') {
    return null;
  }

  if (status === 'failed') {
    return (
      <div className="max-w-7xl mx-auto px-6 mt-6">
        <div className="rounded-lg bg-red-50 border border-red-200 p-4">
          <div className="flex">
            <div className="flex-shrink-0">
              <svg className="h-5 w-5 text-red-400" viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
              </svg>
            </div>
            <div className="ml-3">
              <h3 className="text-sm font-medium text-red-800">Report Generation Failed</h3>
              {error && (
                <div className="mt-2 text-sm text-red-700">
                  <p>{error}</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto px-6 mt-6">
      <div className="rounded-lg bg-blue-50 border border-blue-200 p-4">
        <div className="flex">
          <div className="flex-shrink-0">
            <svg className="h-5 w-5 text-blue-400 animate-spin" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
            </svg>
          </div>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-blue-800">Report Generation In Progress</h3>
            <div className="mt-2 text-sm text-blue-700">
              <p>
                {status === 'pending' && 'Your report is queued for processing...'}
                {status === 'processing' && 'Analyzing data and generating insights...'}
                {status === 'ingesting_data' && 'Collecting data from Google Search Console and GA4...'}
                {status === 'analyzing' && 'Running statistical analysis and ML models...'}
              </p>
              <p className="mt-1 text-xs">This typically takes 2-5 minutes. Feel free to leave this page and return later.</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function ExecutiveSummary({ results }: { results: any }) {
  if (!results?.gameplan?.narrative) {
    return null;
  }

  return (
    <div className="max-w-7xl mx-auto px-6 mt-6">
      <div className="bg-gradient-to-br from-blue-50 to-indigo-50 rounded-xl border border-blue-100 p-8">
        <h2 className="text-2xl font-bold text-gray-900 mb-4">Executive Summary</h2>
        <div className="prose prose-sm max-w-none text-gray-700">
          <p className="text-base leading-relaxed whitespace-pre-wrap">
            {results.gameplan.narrative}
          </p>
        </div>
        
        {results.gameplan?.total_estimated_monthly_click_recovery > 0 && (
          <div className="mt-6 grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="bg-white rounded-lg border border-gray-200 p-4">
              <div className="text-sm font-medium text-gray-600 mb-1">Recoverable Traffic</div>
              <div className="text-2xl font-bold text-gray-900">
                {results.gameplan.total_estimated_monthly_click_recovery.toLocaleString()} clicks/mo
              </div>
            </div>
            {results.gameplan?.total_estimated_monthly_click_growth > 0 && (
              <div className="bg-white rounded-lg border border-gray-200 p-4">
                <div className="text-sm font-medium text-gray-600 mb-1">Growth Opportunity</div>
                <div className="text-2xl font-bold text-gray-900">
                  {results.gameplan.total_estimated_monthly_click_growth.toLocaleString()} clicks/mo
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function ModulesGrid({ reportId, results }: { reportId: string; results: any }) {
  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Detailed Analysis</h2>
        <p className="text-gray-600">
          Comprehensive insights across all areas of your search performance
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Module 1: Traffic Overview & Health */}
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm hover:shadow-md transition-shadow">
          <Module1TrafficOverview reportId={reportId} data={results?.health_trajectory} />
        </div>

        {/* Module 2: Page-Level Triage */}
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm hover:shadow-md transition-shadow p-6">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h3 className="text-lg font-semibold text-gray-900">Page-Level Triage</h3>
              <p className="text-sm text-gray-600 mt-1">Critical pages requiring attention</p>
            </div>
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
              Module 2
            </span>
          </div>
          
          {results?.page_triage ? (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="bg-red-50 rounded-lg p-4 border border-red-100">
                  <div className="text-2xl font-bold text-red-900">
                    {results.page_triage.summary?.critical || 0}
                  </div>
                  <div className="text-xs font-medium text-red-700 mt-1">Critical Pages</div>
                </div>
                <div className="bg-yellow-50 rounded-lg p-4 border border-yellow-100">
                  <div className="text-2xl font-bold text-yellow-900">
                    {results.page_triage.summary?.decaying || 0}
                  </div>
                  <div className="text-xs font-medium text-yellow-700 mt-1">Decaying Pages</div>
                </div>
                <div className="bg-green-50 rounded-lg p-4 border border-green-100">
                  <div className="text-2xl font-bold text-green-900">
                    {results.page_triage.summary?.growing || 0}
                  </div>
                  <div className="text-xs font-medium text-green-700 mt-1">Growing Pages</div>
                </div>
                <div className="bg-blue-50 rounded-lg p-4 border border-blue-100">
                  <div className="text-2xl font-bold text-blue-900">
                    {results.page_triage.summary?.stable || 0}
                  </div>
                  <div className="text-xs font-medium text-blue-700 mt-1">Stable Pages</div>
                </div>
              </div>
              
              {results.page_triage.summary?.total_recoverable_clicks_monthly > 0 && (
                <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
                  <div className="text-sm text-gray-600">Recoverable Monthly Traffic</div>
                  <div className="text-xl font-bold text-gray-900 mt-1">
                    {results.page_triage.summary.total_recoverable_clicks_monthly.toLocaleString()} clicks
                  </div>
                </div>
              )}
            </div>
          ) : (
            <div className="text-center py-8 text-gray-500">
              <p>Data not yet available</p>
            </div>
          )}
        </div>

        {/* Module 3: SERP Landscape */}
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm hover:shadow-md transition-shadow p-6">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h3 className="text-lg font-semibold text-gray-900">SERP Landscape</h3>
              <p className="text-sm text-gray-600 mt-1">Competitive positioning analysis</p>
            </div>
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
              Module 3
            </span>
          </div>
          
          {results?.serp_landscape ? (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="bg-blue-50 rounded-lg p-4 border border-blue-100">
                  <div className="text-2xl font-bold text-blue-900">
                    {results.serp_landscape.keywords_analyzed || 0}
                  </div>
                  <div className="text-xs font-medium text-blue-700 mt-1">Keywords Analyzed</div>
                </div>
                <div className="bg-purple-50 rounded-lg p-4 border border-purple-100">
                  <div className="text-2xl font-bold text-purple-900">
                    {results.serp_landscape.competitors?.length || 0}
                  </div>
                  <div className="text-xs font-medium text-purple-700 mt-1">Key Competitors</div>
                </div>
              </div>
              
              {results.serp_landscape.total_click_share !== undefined && (
                <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
                  <div className="flex justify-between items-end">
                    <div>
                      <div className="text-sm text-gray-600">Current Click Share</div>
                      <div className="text-2xl font-bold text-gray-900 mt-1">
                        {(results.serp_landscape.total_click_share * 100).toFixed(1)}%
                      </div>
                    </div>
                    {results.serp_landscape.click_share_opportunity !== undefined && (
                      <div className="text-right">
                        <div className="text-sm text-gray-600">Opportunity</div>
                        <div className="text-xl font-bold text-green-700 mt-1">
                          {(results.serp_landscape.click_share_opportunity * 100).toFixed(1)}%
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          ) : (
            <div className="text-center py-8 text-gray-500">
              <p>Data not yet available</p>
            </div>
          )}
        </div>

        {/* Module 4: Content Intelligence */}
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm hover:shadow-md transition-shadow p-6">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h3 className="text-lg font-semibold text-gray-900">Content Intelligence</h3>
              <p className="text-sm text-gray-600 mt-1">Content gaps and optimization opportunities</p>
            </div>
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
              Module 4
            </span>
          </div>
          
          {results?.content_intelligence ? (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="bg-orange-50 rounded-lg p-4 border border-orange-100">
                  <div className="text-2xl font-bold text-orange-900">
                    {results.content_intelligence.cannibalization_clusters?.length || 0}
                  </div>
                  <div className="text-xs font-medium text-orange-700 mt-1">Cannibalization Issues</div>
                </div>
                <div className="bg-green-50 rounded-lg p-4 border border-green-100">
                  <div className="text-2xl font-bold text-green-900">
                    {results.content_intelligence.striking_distance?.length || 0}
                  </div>
                  <div className="text-xs font-medium text-green-700 mt-1">Quick Win Keywords</div>
                </div>
              </div>
              
              {results.content_intelligence.thin_content?.length > 0 && (
                <div className="bg-red-50 rounded-lg p-4 border border-red-100">
                  <div className="text-2xl font-bold text-red-900">
                    {results.content_intelligence.thin_content.length}
                  </div>
                  <div className="text-xs font-medium text-red-700 mt-1">Thin Content Pages</div>
                </div>
              )}
            </div>
          ) : (
            <div className="text-center py-8 text-gray-500">
              <p>Data not yet available</p>
            </div>
          )}
        </div>

        {/* Module 5: Action Plan */}
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm hover:shadow-md transition-shadow p-6 lg:col-span-2">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h3 className="text-lg font-semibold text-gray-900">Prioritized Action Plan</h3>
              <p className="text-sm text-gray-600 mt-1">Your roadmap to search growth</p>
            </div>
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
              Module 5
            </span>
          </div>
          
          {results?.gameplan ? (
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div className="bg-red-50 rounded-lg p-4 border border-red-100">
                <div className="text-2xl font-bold text-red-900">
                  {results.gameplan.critical?.length || 0}
                </div>
                <div className="text-xs font-medium text-red-700 mt-1">Critical Fixes</div>
                <div className="text-xs text-red-600 mt-2">Do this week</div>
              </div>
              
              <div className="bg-yellow-50 rounded-lg p-4 border border-yellow-100">
                <div className="text-2xl font-bold text-yellow-900">
                  {results.gameplan.quick_wins?.length || 0}
                </div>
                <div className="text-xs font-medium text-yellow-700 mt-1">Quick Wins</div>
                <div className="text-xs text-yellow-600 mt-2">Do this month</div>
              </div>
              
              <div className="bg-blue-50 rounded-lg p-4 border border-blue-100">
                <div className="text-2xl font-bold text-blue-900">
                  {results.gameplan.strategic?.length || 0}
                </div>
                <div className="text-xs font-medium text-blue-700 mt-1">Strategic Plays</div>
                <div className="text-xs text-blue-600 mt-2">This quarter</div>
              </div>
              
              <div className="bg-purple-50 rounded-lg p-4 border border-purple-100">
                <div className="text-2xl font-bold text-purple-900">
                  {results.gameplan.structural?.length || 0}
                </div>
                <div className="text-xs font-medium text-purple-700 mt-1">Structural Improvements</div>
                <div className="text-xs text-purple-600 mt-2">Ongoing</div>
              </div>
            </div>
          ) : (
            <div className="text-center py-8 text-gray-500">
              <p>Data not yet available</p>
            </div>
          )}
        </div>

        {/* Module 6: Algorithm Impact */}
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm hover:shadow-md transition-shadow p-6">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h3 className="text-lg font-semibold text-gray-900">Algorithm Impact</h3>
              <p className="text-sm text-gray-600 mt-1">How updates affected your site</p>
            </div>
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
              Module 6
            </span>
          </div>
          
          {results?.algorithm_impact ? (
            <div className="space-y-4">
              <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
                <div className="text-2xl font-bold text-gray-900">
                  {results.algorithm_impact.updates_impacting_site?.length || 0}
                </div>
                <div className="text-xs font-medium text-gray-700 mt-1">Algorithm Updates Detected</div>
              </div>
              
              {results.algorithm_impact.vulnerability_score !== undefined && (
                <div className="bg-orange-50 rounded-lg p-4 border border-orange-100">
                  <div className="flex justify-between items-end">
                    <div>
                      <div className="text-sm text-orange-700">Vulnerability Score</div>
                      <div className="text-2xl font-bold text-orange-900 mt-1">
                        {(results.algorithm_impact.vulnerability_score * 100).toFixed(0)}%
                      </div>
                    </div>
                    <div className="text-xs text-orange-600">
                      {results.algorithm_impact.vulnerability_score > 0.7 ? 'High Risk' : 
                       results.algorithm_impact.vulnerability_score > 0.4 ? 'Medium Risk' : 'Low Risk'}
                    </div>
                  </div>
                </div>
              )}
            </div>
          ) : (
            <div className="text-center py-8 text-gray-500">
              <p>Data not yet available</p>
            </div>
          )}
        </div>

        {/* Additional modules placeholder */}
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm hover:shadow-md transition-shadow p-6">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h3 className="text-lg font-semibold text-gray-900">Additional Insights</h3>
              <p className="text-sm text-gray-600 mt-1">More analysis modules coming soon</p>
            </div>
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
              Modules 7-12
            </span>
          </div>
          
          <div className="text-center py-8 text-gray-500">
            <p className="text-sm">Query intent migration, internal link analysis,</p>
            <p className="text-sm">seasonal patterns, and more...</p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default async function ReportPage({ params }: { params: { id: string } }) {
  await checkAuth();
  const report = await getReport(params.id);

  if (!report) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h1 className="text-2xl font-bold text-gray-900 mb-2">Report Not Found</h1>
          <p className="text-gray-600 mb-6">The report you're looking for doesn't exist or you don't have access to it.</p>
          <Link
            href="/dashboard"
            className="inline-flex items-center gap-2 px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 transition-colors"
          >
            <ArrowLeft className="h-4 w-4" />
            Back to Dashboard
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <ReportHeader report={report} />
      
      <ReportStatusBanner status={report.status} error={report.error_message} />
      
      {report.status === 'completed' && report.results && (
        <>
          <ExecutiveSummary results={report.results} />
          <ModulesGrid reportId={report.id} results={report.results} />
        </>
      )}
      
      {report.status === 'processing' && (
        <div className="max-w-7xl mx-auto px-6 py-12">
          <div className="text-center">
            <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-blue-100 mb-4">
              <svg className="h-8 w-8 text-blue-600 animate-spin" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
              </svg>
            </div>
            <h2 className="text-xl font-semibold text-gray-900 mb-2">Analysis in Progress</h2>
            <p className="text-gray-600">
              We're crunching the numbers and generating your insights. This page will auto-refresh when complete.
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
