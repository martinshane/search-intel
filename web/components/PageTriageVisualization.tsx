import React, { useState, useMemo } from 'react';
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  ZAxis,
  Legend
} from 'recharts';

interface PageTriageData {
  url: string;
  bucket: 'growing' | 'stable' | 'decaying' | 'critical';
  current_monthly_clicks: number;
  trend_slope: number;
  projected_page1_loss_date?: string;
  ctr_anomaly: boolean;
  ctr_expected?: number;
  ctr_actual?: number;
  engagement_flag?: string;
  priority_score: number;
  recommended_action: string;
}

interface PageTriageSummary {
  total_pages_analyzed: number;
  growing: number;
  stable: number;
  decaying: number;
  critical: number;
  total_recoverable_clicks_monthly: number;
}

interface PageTriageVisualizationProps {
  pages: PageTriageData[];
  summary: PageTriageSummary;
}

type SortField = 'url' | 'current_monthly_clicks' | 'trend_slope' | 'priority_score';
type SortDirection = 'asc' | 'desc';

const BUCKET_COLORS = {
  growing: '#10b981',
  stable: '#3b82f6',
  decaying: '#f59e0b',
  critical: '#ef4444'
};

const BUCKET_LABELS = {
  growing: 'Growing',
  stable: 'Stable',
  decaying: 'Decaying',
  critical: 'Critical'
};

export default function PageTriageVisualization({ pages, summary }: PageTriageVisualizationProps) {
  const [sortField, setSortField] = useState<SortField>('priority_score');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');
  const [filterBucket, setFilterBucket] = useState<string>('all');

  // Prepare scatter plot data
  const scatterData = useMemo(() => {
    return pages.map(page => ({
      x: page.current_monthly_clicks,
      y: Math.abs(page.trend_slope),
      bucket: page.bucket,
      url: page.url,
      priority_score: page.priority_score,
      recommended_action: page.recommended_action
    }));
  }, [pages]);

  // Filter and sort table data
  const tableData = useMemo(() => {
    let filtered = pages;
    
    if (filterBucket !== 'all') {
      filtered = pages.filter(page => page.bucket === filterBucket);
    }

    const sorted = [...filtered].sort((a, b) => {
      let aVal = a[sortField];
      let bVal = b[sortField];

      if (typeof aVal === 'string') {
        aVal = aVal.toLowerCase();
        bVal = (bVal as string).toLowerCase();
      }

      if (sortDirection === 'asc') {
        return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
      } else {
        return aVal > bVal ? -1 : aVal < bVal ? 1 : 0;
      }
    });

    return sorted;
  }, [pages, sortField, sortDirection, filterBucket]);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('desc');
    }
  };

  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white p-3 border border-gray-200 rounded shadow-lg">
          <p className="font-semibold text-sm mb-1 truncate max-w-xs">{data.url}</p>
          <p className="text-xs text-gray-600">Current clicks: {data.x.toFixed(0)}/mo</p>
          <p className="text-xs text-gray-600">Decay rate: {data.y.toFixed(3)}</p>
          <p className="text-xs text-gray-600">Priority: {data.priority_score.toFixed(1)}</p>
          <p className="text-xs mt-1 capitalize text-gray-700">{BUCKET_LABELS[data.bucket as keyof typeof BUCKET_LABELS]}</p>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="space-y-6">
      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <div className="text-2xl font-bold text-gray-900">{summary.total_pages_analyzed}</div>
          <div className="text-sm text-gray-600">Pages Analyzed</div>
        </div>
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <div className="text-2xl font-bold" style={{ color: BUCKET_COLORS.growing }}>
            {summary.growing}
          </div>
          <div className="text-sm text-gray-600">Growing</div>
        </div>
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <div className="text-2xl font-bold" style={{ color: BUCKET_COLORS.stable }}>
            {summary.stable}
          </div>
          <div className="text-sm text-gray-600">Stable</div>
        </div>
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <div className="text-2xl font-bold" style={{ color: BUCKET_COLORS.decaying }}>
            {summary.decaying}
          </div>
          <div className="text-sm text-gray-600">Decaying</div>
        </div>
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <div className="text-2xl font-bold" style={{ color: BUCKET_COLORS.critical }}>
            {summary.critical}
          </div>
          <div className="text-sm text-gray-600">Critical</div>
        </div>
      </div>

      {/* Recoverable clicks highlight */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="font-semibold text-blue-900">Total Recovery Opportunity</h3>
            <p className="text-sm text-blue-700 mt-1">
              Estimated monthly clicks that can be recovered through recommended actions
            </p>
          </div>
          <div className="text-3xl font-bold text-blue-900">
            {summary.total_recoverable_clicks_monthly.toLocaleString()}
          </div>
        </div>
      </div>

      {/* Scatter plot */}
      <div className="bg-white p-6 rounded-lg border border-gray-200">
        <h3 className="text-lg font-semibold mb-4">
          Page Performance: Current Traffic vs. Decay Rate
        </h3>
        <p className="text-sm text-gray-600 mb-4">
          Pages in the upper-right quadrant (high traffic + high decay) are the highest priority for intervention.
        </p>
        <ResponsiveContainer width="100%" height={400}>
          <ScatterChart
            margin={{ top: 20, right: 30, bottom: 60, left: 60 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              type="number"
              dataKey="x"
              name="Current Monthly Clicks"
              label={{ value: 'Current Monthly Clicks', position: 'bottom', offset: 40 }}
            />
            <YAxis
              type="number"
              dataKey="y"
              name="Decay Rate"
              label={{ value: 'Absolute Decay Rate (clicks/day)', angle: -90, position: 'insideLeft', offset: 10 }}
            />
            <ZAxis range={[50, 400]} />
            <Tooltip content={<CustomTooltip />} />
            <Legend
              verticalAlign="top"
              height={36}
              formatter={(value) => BUCKET_LABELS[value as keyof typeof BUCKET_LABELS]}
            />
            <Scatter name="growing" data={scatterData.filter(d => d.bucket === 'growing')}>
              {scatterData.filter(d => d.bucket === 'growing').map((entry, index) => (
                <Cell key={`cell-growing-${index}`} fill={BUCKET_COLORS.growing} />
              ))}
            </Scatter>
            <Scatter name="stable" data={scatterData.filter(d => d.bucket === 'stable')}>
              {scatterData.filter(d => d.bucket === 'stable').map((entry, index) => (
                <Cell key={`cell-stable-${index}`} fill={BUCKET_COLORS.stable} />
              ))}
            </Scatter>
            <Scatter name="decaying" data={scatterData.filter(d => d.bucket === 'decaying')}>
              {scatterData.filter(d => d.bucket === 'decaying').map((entry, index) => (
                <Cell key={`cell-decaying-${index}`} fill={BUCKET_COLORS.decaying} />
              ))}
            </Scatter>
            <Scatter name="critical" data={scatterData.filter(d => d.bucket === 'critical')}>
              {scatterData.filter(d => d.bucket === 'critical').map((entry, index) => (
                <Cell key={`cell-critical-${index}`} fill={BUCKET_COLORS.critical} />
              ))}
            </Scatter>
          </ScatterChart>
        </ResponsiveContainer>
      </div>

      {/* Detail table */}
      <div className="bg-white rounded-lg border border-gray-200">
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-semibold">Page Detail</h3>
            <div className="flex items-center gap-2">
              <label className="text-sm text-gray-600">Filter:</label>
              <select
                value={filterBucket}
                onChange={(e) => setFilterBucket(e.target.value)}
                className="text-sm border border-gray-300 rounded px-2 py-1"
              >
                <option value="all">All Pages</option>
                <option value="growing">Growing</option>
                <option value="stable">Stable</option>
                <option value="decaying">Decaying</option>
                <option value="critical">Critical</option>
              </select>
            </div>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Status
                </th>
                <th
                  className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('url')}
                >
                  <div className="flex items-center gap-1">
                    URL
                    {sortField === 'url' && (
                      <span className="text-gray-400">{sortDirection === 'asc' ? '↑' : '↓'}</span>
                    )}
                  </div>
                </th>
                <th
                  className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('current_monthly_clicks')}
                >
                  <div className="flex items-center justify-end gap-1">
                    Monthly Clicks
                    {sortField === 'current_monthly_clicks' && (
                      <span className="text-gray-400">{sortDirection === 'asc' ? '↑' : '↓'}</span>
                    )}
                  </div>
                </th>
                <th
                  className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('trend_slope')}
                >
                  <div className="flex items-center justify-end gap-1">
                    Trend
                    {sortField === 'trend_slope' && (
                      <span className="text-gray-400">{sortDirection === 'asc' ? '↑' : '↓'}</span>
                    )}
                  </div>
                </th>
                <th
                  className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('priority_score')}
                >
                  <div className="flex items-center justify-end gap-1">
                    Priority Score
                    {sortField === 'priority_score' && (
                      <span className="text-gray-400">{sortDirection === 'asc' ? '↑' : '↓'}</span>
                    )}
                  </div>
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Recommended Action
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Issues
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {tableData.map((page, index) => (
                <tr key={index} className="hover:bg-gray-50">
                  <td className="px-4 py-3 whitespace-nowrap">
                    <span
                      className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium capitalize"
                      style={{
                        backgroundColor: `${BUCKET_COLORS[page.bucket]}20`,
                        color: BUCKET_COLORS[page.bucket]
                      }}
                    >
                      {page.bucket}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900 max-w-md truncate" title={page.url}>
                    {page.url}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900 text-right whitespace-nowrap">
                    {page.current_monthly_clicks.toFixed(0)}
                  </td>
                  <td className="px-4 py-3 text-sm text-right whitespace-nowrap">
                    <span
                      className={
                        page.trend_slope > 0.1
                          ? 'text-green-600'
                          : page.trend_slope < -0.1
                          ? 'text-red-600'
                          : 'text-gray-600'
                      }
                    >
                      {page.trend_slope > 0 ? '+' : ''}
                      {page.trend_slope.toFixed(3)}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900 text-right whitespace-nowrap font-medium">
                    {page.priority_score.toFixed(1)}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900 max-w-xs">
                    {page.recommended_action.replace(/_/g, ' ')}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-600">
                    <div className="flex flex-col gap-1">
                      {page.ctr_anomaly && (
                        <span className="inline-flex items-center text-xs text-orange-700">
                          ⚠ Low CTR
                          {page.ctr_expected && page.ctr_actual && (
                            <span className="ml-1 text-gray-500">
                              ({(page.ctr_actual * 100).toFixed(1)}% vs {(page.ctr_expected * 100).toFixed(1)}%)
                            </span>
                          )}
                        </span>
                      )}
                      {page.engagement_flag && (
                        <span className="inline-flex items-center text-xs text-red-700">
                          ⚠ {page.engagement_flag.replace(/_/g, ' ')}
                        </span>
                      )}
                      {page.projected_page1_loss_date && (
                        <span className="inline-flex items-center text-xs text-gray-600">
                          📉 P1 loss: {new Date(page.projected_page1_loss_date).toLocaleDateString()}
                        </span>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {tableData.length === 0 && (
          <div className="text-center py-8 text-gray-500">
            No pages match the selected filter.
          </div>
        )}
      </div>
    </div>
  );
}
