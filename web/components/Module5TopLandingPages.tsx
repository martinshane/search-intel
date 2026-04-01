import React, { useState, useMemo } from 'react';
import { TrendingUp, TrendingDown, Minus, ArrowUpDown, ExternalLink } from 'lucide-react';

interface TopLandingPageData {
  url: string;
  sessions: number;
  engagement_rate: number;
  avg_session_duration: number;
  conversion_rate: number;
  sessions_trend: number;
  engagement_trend: number;
  conversion_trend: number;
}

interface Module5Data {
  pages: TopLandingPageData[];
  summary: {
    total_pages_analyzed: number;
    avg_engagement_rate: number;
    avg_session_duration: number;
    avg_conversion_rate: number;
    total_sessions: number;
    high_performers: number;
    underperformers: number;
  };
  benchmarks: {
    engagement_rate_p50: number;
    engagement_rate_p75: number;
    conversion_rate_p50: number;
    conversion_rate_p75: number;
    session_duration_p50: number;
    session_duration_p75: number;
  };
}

interface Module5TopLandingPagesProps {
  data: Module5Data;
}

type SortField = 'sessions' | 'engagement_rate' | 'avg_session_duration' | 'conversion_rate';
type SortDirection = 'asc' | 'desc';

const Module5TopLandingPages: React.FC<Module5TopLandingPagesProps> = ({ data }) => {
  const [sortField, setSortField] = useState<SortField>('sessions');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 20;

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('desc');
    }
    setCurrentPage(1);
  };

  const sortedPages = useMemo(() => {
    const sorted = [...data.pages].sort((a, b) => {
      const aValue = a[sortField];
      const bValue = b[sortField];
      
      if (sortDirection === 'asc') {
        return aValue > bValue ? 1 : -1;
      } else {
        return aValue < bValue ? 1 : -1;
      }
    });
    return sorted;
  }, [data.pages, sortField, sortDirection]);

  const paginatedPages = useMemo(() => {
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    return sortedPages.slice(startIndex, endIndex);
  }, [sortedPages, currentPage]);

  const totalPages = Math.ceil(sortedPages.length / itemsPerPage);

  const formatDuration = (seconds: number): string => {
    if (seconds < 60) {
      return `${Math.round(seconds)}s`;
    }
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = Math.round(seconds % 60);
    return `${minutes}m ${remainingSeconds}s`;
  };

  const formatPercent = (value: number): string => {
    return `${(value * 100).toFixed(1)}%`;
  };

  const formatNumber = (value: number): string => {
    return new Intl.NumberFormat('en-US').format(value);
  };

  const getTrendIcon = (trend: number) => {
    if (trend > 0.05) {
      return <TrendingUp className="w-4 h-4 text-green-600" />;
    } else if (trend < -0.05) {
      return <TrendingDown className="w-4 h-4 text-red-600" />;
    } else {
      return <Minus className="w-4 h-4 text-gray-400" />;
    }
  };

  const getTrendColor = (trend: number): string => {
    if (trend > 0.05) return 'text-green-600';
    if (trend < -0.05) return 'text-red-600';
    return 'text-gray-500';
  };

  const getPerformanceBadge = (page: TopLandingPageData): { label: string; color: string } | null => {
    const { engagement_rate, conversion_rate, avg_session_duration } = page;
    const { engagement_rate_p75, conversion_rate_p75, session_duration_p75 } = data.benchmarks;

    const highPerformer = 
      engagement_rate >= engagement_rate_p75 &&
      conversion_rate >= conversion_rate_p75 &&
      avg_session_duration >= session_duration_p75;

    if (highPerformer) {
      return { label: 'High Performer', color: 'bg-green-100 text-green-800 border-green-200' };
    }

    const underperformer =
      engagement_rate < data.benchmarks.engagement_rate_p50 &&
      conversion_rate < data.benchmarks.conversion_rate_p50;

    if (underperformer) {
      return { label: 'Needs Attention', color: 'bg-red-100 text-red-800 border-red-200' };
    }

    return null;
  };

  const SortableHeader: React.FC<{ field: SortField; children: React.ReactNode }> = ({ field, children }) => (
    <th 
      className="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
      onClick={() => handleSort(field)}
    >
      <div className="flex items-center space-x-1">
        <span>{children}</span>
        <ArrowUpDown className={`w-3 h-3 ${sortField === field ? 'text-blue-600' : 'text-gray-400'}`} />
        {sortField === field && (
          <span className="text-blue-600 text-xs">
            {sortDirection === 'asc' ? '↑' : '↓'}
          </span>
        )}
      </div>
    </th>
  );

  return (
    <div className="space-y-6">
      {/* Summary Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg p-4 border border-blue-200">
          <div className="text-sm text-blue-700 font-medium mb-1">Total Sessions</div>
          <div className="text-2xl font-bold text-blue-900">{formatNumber(data.summary.total_sessions)}</div>
          <div className="text-xs text-blue-600 mt-1">{data.summary.total_pages_analyzed} pages analyzed</div>
        </div>

        <div className="bg-gradient-to-br from-green-50 to-green-100 rounded-lg p-4 border border-green-200">
          <div className="text-sm text-green-700 font-medium mb-1">Avg Engagement Rate</div>
          <div className="text-2xl font-bold text-green-900">{formatPercent(data.summary.avg_engagement_rate)}</div>
          <div className="text-xs text-green-600 mt-1">
            Median: {formatPercent(data.benchmarks.engagement_rate_p50)}
          </div>
        </div>

        <div className="bg-gradient-to-br from-purple-50 to-purple-100 rounded-lg p-4 border border-purple-200">
          <div className="text-sm text-purple-700 font-medium mb-1">Avg Session Duration</div>
          <div className="text-2xl font-bold text-purple-900">{formatDuration(data.summary.avg_session_duration)}</div>
          <div className="text-xs text-purple-600 mt-1">
            Median: {formatDuration(data.benchmarks.session_duration_p50)}
          </div>
        </div>

        <div className="bg-gradient-to-br from-orange-50 to-orange-100 rounded-lg p-4 border border-orange-200">
          <div className="text-sm text-orange-700 font-medium mb-1">Avg Conversion Rate</div>
          <div className="text-2xl font-bold text-orange-900">{formatPercent(data.summary.avg_conversion_rate)}</div>
          <div className="text-xs text-orange-600 mt-1">
            Median: {formatPercent(data.benchmarks.conversion_rate_p50)}
          </div>
        </div>
      </div>

      {/* Performance Distribution */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <h3 className="text-sm font-semibold text-gray-900 mb-3">Performance Distribution</h3>
        <div className="grid grid-cols-2 gap-4">
          <div className="flex items-center justify-between p-3 bg-green-50 rounded-lg border border-green-200">
            <div>
              <div className="text-xs text-green-700 font-medium">High Performers</div>
              <div className="text-lg font-bold text-green-900">{data.summary.high_performers}</div>
            </div>
            <div className="text-2xl text-green-600">✓</div>
          </div>
          <div className="flex items-center justify-between p-3 bg-red-50 rounded-lg border border-red-200">
            <div>
              <div className="text-xs text-red-700 font-medium">Needs Attention</div>
              <div className="text-lg font-bold text-red-900">{data.summary.underperformers}</div>
            </div>
            <div className="text-2xl text-red-600">!</div>
          </div>
        </div>
      </div>

      {/* Landing Pages Table */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
          <h3 className="text-lg font-semibold text-gray-900">Top Landing Pages Performance</h3>
          <p className="text-sm text-gray-600 mt-1">
            Showing {((currentPage - 1) * itemsPerPage) + 1}-{Math.min(currentPage * itemsPerPage, sortedPages.length)} of {sortedPages.length} pages
          </p>
        </div>

        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider w-2/5">
                  Landing Page
                </th>
                <SortableHeader field="sessions">Sessions</SortableHeader>
                <SortableHeader field="engagement_rate">Engagement Rate</SortableHeader>
                <SortableHeader field="avg_session_duration">Avg Duration</SortableHeader>
                <SortableHeader field="conversion_rate">Conversion Rate</SortableHeader>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {paginatedPages.map((page, index) => {
                const badge = getPerformanceBadge(page);
                return (
                  <tr key={index} className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-4">
                      <div className="flex flex-col">
                        <div className="flex items-center space-x-2">
                          <a
                            href={page.url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-sm text-blue-600 hover:text-blue-800 hover:underline font-medium max-w-md truncate"
                            title={page.url}
                          >
                            {page.url}
                          </a>
                          <ExternalLink className="w-3 h-3 text-gray-400 flex-shrink-0" />
                        </div>
                        {badge && (
                          <span className={`text-xs px-2 py-0.5 rounded-full border inline-block mt-1 w-fit ${badge.color}`}>
                            {badge.label}
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-4">
                      <div className="flex items-center space-x-2">
                        <span className="text-sm font-semibold text-gray-900">
                          {formatNumber(page.sessions)}
                        </span>
                        {getTrendIcon(page.sessions_trend)}
                      </div>
                      {Math.abs(page.sessions_trend) > 0.05 && (
                        <div className={`text-xs font-medium mt-0.5 ${getTrendColor(page.sessions_trend)}`}>
                          {page.sessions_trend > 0 ? '+' : ''}{formatPercent(page.sessions_trend)}
                        </div>
                      )}
                    </td>
                    <td className="px-4 py-4">
                      <div className="flex items-center space-x-2">
                        <span className="text-sm font-semibold text-gray-900">
                          {formatPercent(page.engagement_rate)}
                        </span>
                        {getTrendIcon(page.engagement_trend)}
                      </div>
                      {Math.abs(page.engagement_trend) > 0.05 && (
                        <div className={`text-xs font-medium mt-0.5 ${getTrendColor(page.engagement_trend)}`}>
                          {page.engagement_trend > 0 ? '+' : ''}{formatPercent(page.engagement_trend)}
                        </div>
                      )}
                      {page.engagement_rate < data.benchmarks.engagement_rate_p50 && (
                        <div className="text-xs text-red-600 mt-0.5">Below median</div>
                      )}
                      {page.engagement_rate >= data.benchmarks.engagement_rate_p75 && (
                        <div className="text-xs text-green-600 mt-0.5">Top 25%</div>
                      )}
                    </td>
                    <td className="px-4 py-4">
                      <div className="text-sm font-semibold text-gray-900">
                        {formatDuration(page.avg_session_duration)}
                      </div>
                      {page.avg_session_duration < data.benchmarks.session_duration_p50 && (
                        <div className="text-xs text-red-600 mt-0.5">Below median</div>
                      )}
                      {page.avg_session_duration >= data.benchmarks.session_duration_p75 && (
                        <div className="text-xs text-green-600 mt-0.5">Top 25%</div>
                      )}
                    </td>
                    <td className="px-4 py-4">
                      <div className="flex items-center space-x-2">
                        <span className="text-sm font-semibold text-gray-900">
                          {formatPercent(page.conversion_rate)}
                        </span>
                        {getTrendIcon(page.conversion_trend)}
                      </div>
                      {Math.abs(page.conversion_trend) > 0.05 && (
                        <div className={`text-xs font-medium mt-0.5 ${getTrendColor(page.conversion_trend)}`}>
                          {page.conversion_trend > 0 ? '+' : ''}{formatPercent(page.conversion_trend)}
                        </div>
                      )}
                      {page.conversion_rate < data.benchmarks.conversion_rate_p50 && (
                        <div className="text-xs text-red-600 mt-0.5">Below median</div>
                      )}
                      {page.conversion_rate >= data.benchmarks.conversion_rate_p75 && (
                        <div className="text-xs text-green-600 mt-0.5">Top 25%</div>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="px-6 py-4 border-t border-gray-200 bg-gray-50">
            <div className="flex items-center justify-between">
              <button
                onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                disabled={currentPage === 1}
                className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                Previous
              </button>
              
              <div className="flex items-center space-x-2">
                {Array.from({ length: totalPages }, (_, i) => i + 1).map(pageNum => {
                  if (
                    pageNum === 1 ||
                    pageNum === totalPages ||
                    (pageNum >= currentPage - 1 && pageNum <= currentPage + 1)
                  ) {
                    return (
                      <button
                        key={pageNum}
                        onClick={() => setCurrentPage(pageNum)}
                        className={`px-3 py-1 text-sm font-medium rounded-md transition-colors ${
                          currentPage === pageNum
                            ? 'bg-blue-600 text-white'
                            : 'text-gray-700 bg-white border border-gray-300 hover:bg-gray-50'
                        }`}
                      >
                        {pageNum}
                      </button>
                    );
                  } else if (
                    pageNum === currentPage - 2 ||
                    pageNum === currentPage + 2
                  ) {
                    return (
                      <span key={pageNum} className="px-2 text-gray-500">
                        ...
                      </span>
                    );
                  }
                  return null;
                })}
              </div>

              <button
                onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                disabled={currentPage === totalPages}
                className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                Next
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Insights */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <h4 className="text-sm font-semibold text-blue-900 mb-2">Key Insights</h4>
        <ul className="space-y-2 text-sm text-blue-800">
          <li className="flex items-start">
            <span className="mr-2">•</span>
            <span>
              <strong>{data.summary.high_performers}</strong> pages are high performers (top 25% in all metrics)
            </span>
          </li>
          <li className="flex items-start">
            <span className="mr-2">•</span>
            <span>
              <strong>{data.summary.underperformers}</strong> pages need attention (below median in engagement and conversion)
            </span>
          </li>
          <li className="flex items-start">
            <span className="mr-2">•</span>
            <span>
              Average engagement rate is <strong>{formatPercent(data.summary.avg_engagement_rate)}</strong> compared to median of <strong>{formatPercent(data.benchmarks.engagement_rate_p50)}</strong>
            </span>
          </li>
          <li className="flex items-start">
            <span className="mr-2">•</span>
            <span>
              Focus optimization efforts on pages below median engagement to maximize overall site performance
            </span>
          </li>
        </ul>
      </div>
    </div>
  );
};

export default Module5TopLandingPages;
