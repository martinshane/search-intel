import React from 'react';
import { ReportData } from '../types/report';
import Module1Charts from './Module1Charts';

interface ReportProps {
  report: ReportData | null;
  loading: boolean;
  error: string | null;
}

const Report: React.FC<ReportProps> = ({ report, loading, error }) => {
  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mb-4"></div>
          <p className="text-gray-600">Generating your Search Intelligence Report...</p>
          <p className="text-sm text-gray-500 mt-2">This may take 2-5 minutes</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="bg-red-50 border border-red-200 rounded-lg p-6 max-w-md">
          <h3 className="text-red-800 font-semibold mb-2">Error</h3>
          <p className="text-red-700">{error}</p>
        </div>
      </div>
    );
  }

  if (!report) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-center text-gray-500">
          <p>Connect your Google Search Console and GA4 to generate a report</p>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto px-4 py-8">
      <header className="mb-8">
        <h1 className="text-4xl font-bold text-gray-900 mb-2">
          Search Intelligence Report
        </h1>
        <p className="text-gray-600">
          Generated on {new Date(report.generated_at).toLocaleDateString()}
        </p>
        {report.site_url && (
          <p className="text-gray-600">
            Site: <span className="font-medium">{report.site_url}</span>
          </p>
        )}
      </header>

      <div className="space-y-8">
        {/* Module 1: Health & Trajectory */}
        {report.modules?.module1 && (
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">
              1. Health & Trajectory
            </h2>
            <Module1Charts data={report.modules.module1} />
          </section>
        )}

        {/* Module 2: Page-Level Triage */}
        {report.modules?.module2 && (
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">
              2. Page-Level Triage
            </h2>
            
            {report.modules.module2.summary && (
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
                <div className="bg-gray-50 rounded p-4">
                  <div className="text-sm text-gray-600">Total Pages</div>
                  <div className="text-2xl font-bold text-gray-900">
                    {report.modules.module2.summary.total_pages_analyzed}
                  </div>
                </div>
                <div className="bg-green-50 rounded p-4">
                  <div className="text-sm text-gray-600">Growing</div>
                  <div className="text-2xl font-bold text-green-700">
                    {report.modules.module2.summary.growing}
                  </div>
                </div>
                <div className="bg-blue-50 rounded p-4">
                  <div className="text-sm text-gray-600">Stable</div>
                  <div className="text-2xl font-bold text-blue-700">
                    {report.modules.module2.summary.stable}
                  </div>
                </div>
                <div className="bg-yellow-50 rounded p-4">
                  <div className="text-sm text-gray-600">Decaying</div>
                  <div className="text-2xl font-bold text-yellow-700">
                    {report.modules.module2.summary.decaying}
                  </div>
                </div>
                <div className="bg-red-50 rounded p-4">
                  <div className="text-sm text-gray-600">Critical</div>
                  <div className="text-2xl font-bold text-red-700">
                    {report.modules.module2.summary.critical}
                  </div>
                </div>
              </div>
            )}

            {report.modules.module2.pages && report.modules.module2.pages.length > 0 && (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Page
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Status
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Monthly Clicks
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Trend
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Priority
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Action
                      </th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {report.modules.module2.pages.slice(0, 10).map((page, idx) => (
                      <tr key={idx} className="hover:bg-gray-50">
                        <td className="px-4 py-3 text-sm text-gray-900 max-w-xs truncate">
                          {page.url}
                        </td>
                        <td className="px-4 py-3 text-sm">
                          <span
                            className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${
                              page.bucket === 'growing'
                                ? 'bg-green-100 text-green-800'
                                : page.bucket === 'stable'
                                ? 'bg-blue-100 text-blue-800'
                                : page.bucket === 'decaying'
                                ? 'bg-yellow-100 text-yellow-800'
                                : 'bg-red-100 text-red-800'
                            }`}
                          >
                            {page.bucket}
                          </span>
                        </td>
                        <td className="px-4 py-3 text-sm text-gray-900">
                          {page.current_monthly_clicks?.toLocaleString() || 'N/A'}
                        </td>
                        <td className="px-4 py-3 text-sm">
                          <span
                            className={
                              (page.trend_slope || 0) > 0
                                ? 'text-green-600'
                                : 'text-red-600'
                            }
                          >
                            {page.trend_slope !== undefined
                              ? page.trend_slope.toFixed(2)
                              : 'N/A'}
                          </span>
                        </td>
                        <td className="px-4 py-3 text-sm text-gray-900">
                          {page.priority_score !== undefined
                            ? page.priority_score.toFixed(1)
                            : 'N/A'}
                        </td>
                        <td className="px-4 py-3 text-sm text-gray-700">
                          {page.recommended_action || 'N/A'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        )}

        {/* Module 3: SERP Landscape */}
        {report.modules?.module3 && (
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">
              3. SERP Landscape Analysis
            </h2>
            
            {report.modules.module3.total_click_share !== undefined && (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                <div className="bg-blue-50 rounded p-4">
                  <div className="text-sm text-gray-600">Current Click Share</div>
                  <div className="text-2xl font-bold text-blue-700">
                    {(report.modules.module3.total_click_share * 100).toFixed(1)}%
                  </div>
                </div>
                {report.modules.module3.click_share_opportunity !== undefined && (
                  <div className="bg-green-50 rounded p-4">
                    <div className="text-sm text-gray-600">Click Share Opportunity</div>
                    <div className="text-2xl font-bold text-green-700">
                      {(report.modules.module3.click_share_opportunity * 100).toFixed(1)}%
                    </div>
                  </div>
                )}
              </div>
            )}

            {report.modules.module3.competitors && report.modules.module3.competitors.length > 0 && (
              <div className="mb-6">
                <h3 className="text-lg font-semibold text-gray-900 mb-3">
                  Primary Competitors
                </h3>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                  {report.modules.module3.competitors.slice(0, 6).map((comp, idx) => (
                    <div key={idx} className="border border-gray-200 rounded p-4">
                      <div className="font-medium text-gray-900 mb-2">
                        {comp.domain}
                      </div>
                      <div className="text-sm text-gray-600 space-y-1">
                        <div>Shared keywords: {comp.keywords_shared}</div>
                        <div>Avg position: {comp.avg_position?.toFixed(1) || 'N/A'}</div>
                        <div>
                          Threat:{' '}
                          <span
                            className={
                              comp.threat_level === 'high'
                                ? 'text-red-600 font-medium'
                                : comp.threat_level === 'medium'
                                ? 'text-yellow-600 font-medium'
                                : 'text-green-600 font-medium'
                            }
                          >
                            {comp.threat_level}
                          </span>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {report.modules.module3.serp_feature_displacement &&
              report.modules.module3.serp_feature_displacement.length > 0 && (
                <div>
                  <h3 className="text-lg font-semibold text-gray-900 mb-3">
                    SERP Feature Displacement
                  </h3>
                  <div className="overflow-x-auto">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                            Keyword
                          </th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                            Organic Pos
                          </th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                            Visual Pos
                          </th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                            Features Above
                          </th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                            CTR Impact
                          </th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {report.modules.module3.serp_feature_displacement
                          .slice(0, 10)
                          .map((item, idx) => (
                            <tr key={idx} className="hover:bg-gray-50">
                              <td className="px-4 py-3 text-sm text-gray-900">
                                {item.keyword}
                              </td>
                              <td className="px-4 py-3 text-sm text-gray-900">
                                {item.organic_position}
                              </td>
                              <td className="px-4 py-3 text-sm text-red-600 font-medium">
                                {item.visual_position}
                              </td>
                              <td className="px-4 py-3 text-sm text-gray-700">
                                {item.features_above?.join(', ') || 'N/A'}
                              </td>
                              <td className="px-4 py-3 text-sm text-red-600">
                                {item.estimated_ctr_impact !== undefined
                                  ? `${(item.estimated_ctr_impact * 100).toFixed(1)}%`
                                  : 'N/A'}
                              </td>
                            </tr>
                          ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
          </section>
        )}

        {/* Module 4: Content Intelligence */}
        {report.modules?.module4 && (
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">
              4. Content Intelligence
            </h2>

            {report.modules.module4.cannibalization_clusters &&
              report.modules.module4.cannibalization_clusters.length > 0 && (
                <div className="mb-6">
                  <h3 className="text-lg font-semibold text-gray-900 mb-3">
                    Cannibalization Issues
                  </h3>
                  {report.modules.module4.cannibalization_clusters
                    .slice(0, 5)
                    .map((cluster, idx) => (
                      <div
                        key={idx}
                        className="border border-yellow-200 bg-yellow-50 rounded p-4 mb-3"
                      >
                        <div className="font-medium text-gray-900 mb-2">
                          Query Group: {cluster.query_group}
                        </div>
                        <div className="text-sm text-gray-700 space-y-1">
                          <div>
                            Competing pages:{' '}
                            {cluster.pages?.join(', ') || 'N/A'}
                          </div>
                          <div>Shared queries: {cluster.shared_queries}</div>
                          <div>
                            Impressions affected:{' '}
                            {cluster.total_impressions_affected?.toLocaleString() || 'N/A'}
                          </div>
                          <div className="mt-2 font-medium text-yellow-800">
                            Recommendation: {cluster.recommendation}
                            {cluster.keep_page && ` (Keep: ${cluster.keep_page})`}
                          </div>
                        </div>
                      </div>
                    ))}
                </div>
              )}

            {report.modules.module4.striking_distance &&
              report.modules.module4.striking_distance.length > 0 && (
                <div className="mb-6">
                  <h3 className="text-lg font-semibold text-gray-900 mb-3">
                    Striking Distance Opportunities
                  </h3>
                  <div className="overflow-x-auto">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                            Query
                          </th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                            Position
                          </th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                            Impressions
                          </th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                            Click Gain
                          </th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                            Intent
                          </th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {report.modules.module4.striking_distance
                          .slice(0, 10)
                          .map((item, idx) => (
                            <tr key={idx} className="hover:bg-gray-50">
                              <td className="px-4 py-3 text-sm text-gray-900">
                                {item.query}
                              </td>
                              <td className="px-4 py-3 text-sm text-gray-900">
                                {item.current_position?.toFixed(1) || 'N/A'}
                              </td>
                              <td className="px-4 py-3 text-sm text-gray-900">
                                {item.impressions?.toLocaleString() || 'N/A'}
                              </td>
                              <td className="px-4 py-3 text-sm text-green-600 font-medium">
                                +{item.estimated_click_gain_if_top5?.toLocaleString() || 'N/A'}
                              </td>
                              <td className="px-4 py-3 text-sm text-gray-700">
                                {item.intent || 'N/A'}
                              </td>
                            </tr>
                          ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
          </section>
        )}

        {/* Module 5: The Gameplan */}
        {report.modules?.module5 && (
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">
              5. The Gameplan
            </h2>

            {report.modules.module5.narrative && (
              <div className="bg-blue-50 border border-blue-200 rounded p-4 mb-6">
                <p className="text-gray-800 whitespace-pre-wrap">
                  {report.modules.module5.narrative}
                </p>
              </div>
            )}

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
              {report.modules.module5.total_estimated_monthly_click_recovery !==
                undefined && (
                <div className="bg-green-50 rounded p-4">
                  <div className="text-sm text-gray-600">
                    Estimated Recovery Potential
                  </div>
                  <div className="text-2xl font-bold text-green-700">
                    {report.modules.module5.total_estimated_monthly_click_recovery.toLocaleString()}{' '}
                    clicks/mo
                  </div>
                </div>
              )}
              {report.modules.module5.total_estimated_monthly_click_growth !==
                undefined && (
                <div className="bg-blue-50 rounded p-4">
                  <div className="text-sm text-gray-600">
                    Estimated Growth Potential
                  </div>
                  <div className="text-2xl font-bold text-blue-700">
                    {report.modules.module5.total_estimated_monthly_click_growth.toLocaleString()}{' '}
                    clicks/mo
                  </div>
                </div>
              )}
            </div>

            {report.modules.module5.critical &&
              report.modules.module5.critical.length > 0 && (
                <div className="mb-6">
                  <h3 className="text-lg font-semibold text-red-700 mb-3">
                    Critical Fixes (Do This Week)
                  </h3>
                  <div className="space-y-3">
                    {report.modules.module5.critical.map((item, idx) => (
                      <div
                        key={idx}
                        className="border border-red-200 bg-red-50 rounded p-4"
                      >
                        <div className="font-medium text-gray-900 mb-2">
                          {item.action}
                        </div>
                        <div className="grid grid-cols-2 md:grid-cols-3 gap-2 text-sm text-gray-700">
                          <div>
                            Impact:{' '}
                            <span className="font-medium">
                              {item.impact} clicks/mo
                            </span>
                          </div>
                          <div>
                            Effort:{' '}
                            <span className="font-medium">{item.effort}</span>
                          </div>
                          {item.dependencies && (
                            <div className="col-span-2 md:col-span-1">
                              Dependencies: {item.dependencies}
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

            {report.modules.module5.quick_wins &&
              report.modules.module5.quick_wins.length > 0 && (
                <div className="mb-6">
                  <h3 className="text-lg font-semibold text-yellow-700 mb-3">
                    Quick Wins (Do This Month)
                  </h3>
                  <div className="space-y-3">
                    {report.modules.module5.quick_wins.map((item, idx) => (
                      <div
                        key={idx}
                        className="border border-yellow-200 bg-yellow-50 rounded p-4"
                      >
                        <div className="font-medium text-gray-900 mb-2">
                          {item.action}
                        </div>
                        <div className="grid grid-cols-2 md:grid-cols-3 gap-2 text-sm text-gray-700">
                          <div>
                            Impact:{' '}
                            <span className="font-medium">
                              {item.impact} clicks/mo
                            </span>
                          </div>
                          <div>
                            Effort:{' '}
                            <span className="font-medium">{item.effort}</span>
                          </div>
                          {item.dependencies && (
                            <div className="col-span-2 md:col-span-1">
                              Dependencies: {item.dependencies}
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

            {report.modules.module5.strategic &&
              report.modules.module5.strategic.length > 0 && (
                <div className="mb-6">
                  <h3 className="text-lg font-semibold text-blue-700 mb-3">
                    Strategic Plays (This Quarter)
                  </h3>
                  <div className="space-y-3">
                    {report.modules.module5.strategic.map((item, idx) => (
                      <div
                        key={idx}
                        className="border border-blue-200 bg-blue-50 rounded p-4"
                      >
                        <div className="font-medium text-gray-900 mb-2">
                          {item.action}
                        </div>
                        <div className="grid grid-cols-2 md:grid-cols-3 gap-2 text-sm text-gray-700">
                          <div>
                            Impact:{' '}
                            <span className="font-medium">
                              {item.impact} clicks/mo
                            </span>
                          </div>
                          <div>
                            Effort:{' '}
                            <span className="font-medium">{item.effort}</span>
                          </div>
                          {item.dependencies && (
                            <div className="col-span-2 md:col-span-1">
                              Dependencies: {item.dependencies}
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

            {report.modules.module5.structural &&
              report.modules.module5.structural.length > 0 && (
                <div>
                  <h3 className="text-lg font-semibold text-gray-700 mb-3">
                    Structural Improvements (Ongoing)
                  </h3>
                  <div className="space-y-3">
                    {report.modules.module5.structural.map((item, idx) => (
                      <div
                        key={idx}
                        className="border border-gray-200 bg-gray-50 rounded p-4"
                      >
                        <div className="font-medium text-gray-900 mb-2">
                          {item.action}
                        </div>
                        <div className="grid grid-cols-2 md:grid-cols-3 gap-2 text-sm text-gray-700">
                          <div>
                            Impact:{' '}
                            <span className="font-medium">
                              {item.impact} clicks/mo
                            </span>
                          </div>
                          <div>
                            Effort:{' '}
                            <span className="font-medium">{item.effort}</span>
                          </div>
                          {item.dependencies && (
                            <div className="col-span-2 md:col-span-1">
                              Dependencies: {item.dependencies}
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
          </section>
        )}

        {/* Additional modules placeholders */}
        {report.modules?.module6 && (
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">
              6. Algorithm Update Impact Analysis
            </h2>
            <pre className="bg-gray-50 p-4 rounded overflow-auto text-sm">
              {JSON.stringify(report.modules.module6, null, 2)}
            </pre>
          </section>
        )}

        {report.modules?.module7 && (
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">
              7. Query Intent Migration Tracking
            </h2>
            <pre className="bg-gray-50 p-4 rounded overflow-auto text-sm">
              {JSON.stringify(report.modules.module7, null, 2)}
            </pre>
          </section>
        )}

        {report.modules?.module8 && (
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">
              8. Internal Link Network Analysis
            </h2>
            <pre className="bg-gray-50 p-4 rounded overflow-auto text-sm">
              {JSON.stringify(report.modules.module8, null, 2)}
            </pre>
          </section>
        )}

        {report.modules?.module9 && (
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">
              9. Seasonality & Cyclical Pattern Detection
            </h2>
            <pre className="bg-gray-50 p-4 rounded overflow-auto text-sm">
              {JSON.stringify(report.modules.module9, null, 2)}
            </pre>
          </section>
        )}

        {report.modules?.module10 && (
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">
              10. Conversion Pathway Analysis
            </h2>
            <pre className="bg-gray-50 p-4 rounded overflow-auto text-sm">
              {JSON.stringify(report.modules.module10, null, 2)}
            </pre>
          </section>
        )}

        {report.modules?.module11 && (
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">
              11. Technical SEO Health Score
            </h2>
            <pre className="bg-gray-50 p-4 rounded overflow-auto text-sm">
              {JSON.stringify(report.modules.module11, null, 2)}
            </pre>
          </section>
        )}

        {report.modules?.module12 && (
          <section className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">
              12. Competitive Moat Analysis
            </h2>
            <pre className="bg-gray-50 p-4 rounded overflow-auto text-sm">
              {JSON.stringify(report.modules.module12, null, 2)}
            </pre>
          </section>
        )}
      </div>
    </div>
  );
};

export default Report;