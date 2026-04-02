import React from 'react';
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,
  ReferenceLine
} from 'recharts';

interface Module5Data {
  ctr_analysis: {
    impressions_vs_ctr_scatter: Array<{
      url: string;
      impressions: number;
      ctr: number;
      expected_ctr: number;
      position: number;
      bucket: 'overperforming' | 'expected' | 'underperforming' | 'critical';
    }>;
    top_performers: Array<{
      url: string;
      impressions: number;
      ctr: number;
      expected_ctr: number;
      ctr_lift_pct: number;
      position: number;
    }>;
    bottom_performers: Array<{
      url: string;
      impressions: number;
      ctr: number;
      expected_ctr: number;
      ctr_gap_pct: number;
      position: number;
      issue: string;
    }>;
    opportunities: Array<{
      url: string;
      impressions: number;
      ctr: number;
      expected_ctr: number;
      estimated_click_gain: number;
      recommended_action: string;
      action_type: 'title_optimization' | 'meta_description_optimization' | 'schema_addition' | 'content_snippet_optimization';
      effort: 'low' | 'medium' | 'high';
      priority_score: number;
    }>;
  };
  summary: {
    total_pages_analyzed: number;
    overperforming_pages: number;
    underperforming_pages: number;
    critical_pages: number;
    total_opportunity_clicks: number;
    avg_ctr_gap: number;
  };
}

interface Module5ChartProps {
  data: Module5Data;
}

const Module5Chart: React.FC<Module5ChartProps> = ({ data }) => {
  // Color mapping for buckets
  const bucketColors = {
    overperforming: '#10b981',
    expected: '#6b7280',
    underperforming: '#f59e0b',
    critical: '#ef4444'
  };

  // Prepare scatter data with color
  const scatterData = data.ctr_analysis.impressions_vs_ctr_scatter.map(point => ({
    ...point,
    color: bucketColors[point.bucket]
  }));

  // Calculate expected CTR trend line points
  const maxImpressions = Math.max(...scatterData.map(d => d.impressions));
  const minImpressions = Math.min(...scatterData.map(d => d.impressions));

  // Custom tooltip for scatter plot
  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white p-3 border border-gray-200 rounded shadow-lg text-sm">
          <p className="font-semibold text-gray-900 mb-1 truncate max-w-xs">
            {data.url}
          </p>
          <div className="space-y-1 text-xs">
            <p>
              <span className="text-gray-600">Impressions:</span>{' '}
              <span className="font-medium">{data.impressions.toLocaleString()}</span>
            </p>
            <p>
              <span className="text-gray-600">CTR:</span>{' '}
              <span className="font-medium">{(data.ctr * 100).toFixed(2)}%</span>
            </p>
            <p>
              <span className="text-gray-600">Expected CTR:</span>{' '}
              <span className="font-medium">{(data.expected_ctr * 100).toFixed(2)}%</span>
            </p>
            <p>
              <span className="text-gray-600">Avg Position:</span>{' '}
              <span className="font-medium">{data.position.toFixed(1)}</span>
            </p>
            <p>
              <span className="text-gray-600">Status:</span>{' '}
              <span className={`font-medium ${
                data.bucket === 'overperforming' ? 'text-green-600' :
                data.bucket === 'critical' ? 'text-red-600' :
                data.bucket === 'underperforming' ? 'text-orange-600' :
                'text-gray-600'
              }`}>
                {data.bucket.replace('_', ' ')}
              </span>
            </p>
          </div>
        </div>
      );
    }
    return null;
  };

  // Format large numbers
  const formatNumber = (num: number) => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
  };

  // Truncate URL for display
  const truncateUrl = (url: string, maxLength: number = 50) => {
    if (url.length <= maxLength) return url;
    return url.substring(0, maxLength - 3) + '...';
  };

  // Sort opportunities by priority score
  const sortedOpportunities = [...data.ctr_analysis.opportunities].sort(
    (a, b) => b.priority_score - a.priority_score
  );

  // Get action type icon
  const getActionIcon = (actionType: string) => {
    switch (actionType) {
      case 'title_optimization':
        return '📝';
      case 'meta_description_optimization':
        return '📄';
      case 'schema_addition':
        return '🏷️';
      case 'content_snippet_optimization':
        return '✨';
      default:
        return '🔧';
    }
  };

  // Get effort badge color
  const getEffortColor = (effort: string) => {
    switch (effort) {
      case 'low':
        return 'bg-green-100 text-green-800';
      case 'medium':
        return 'bg-yellow-100 text-yellow-800';
      case 'high':
        return 'bg-red-100 text-red-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  return (
    <div className="space-y-8">
      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <div className="text-sm text-gray-600 mb-1">Total Pages Analyzed</div>
          <div className="text-2xl font-bold text-gray-900">
            {data.summary.total_pages_analyzed.toLocaleString()}
          </div>
        </div>
        
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <div className="text-sm text-gray-600 mb-1">Underperforming</div>
          <div className="text-2xl font-bold text-orange-600">
            {data.summary.underperforming_pages.toLocaleString()}
          </div>
          <div className="text-xs text-gray-500 mt-1">
            + {data.summary.critical_pages} critical
          </div>
        </div>
        
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <div className="text-sm text-gray-600 mb-1">Opportunity Clicks/Mo</div>
          <div className="text-2xl font-bold text-blue-600">
            {data.summary.total_opportunity_clicks.toLocaleString()}
          </div>
        </div>
        
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <div className="text-sm text-gray-600 mb-1">Avg CTR Gap</div>
          <div className="text-2xl font-bold text-gray-900">
            {(data.summary.avg_ctr_gap * 100).toFixed(1)}%
          </div>
        </div>
      </div>

      {/* Scatter Plot */}
      <div className="bg-white p-6 rounded-lg border border-gray-200">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">
          CTR vs Impressions Analysis
        </h3>
        <p className="text-sm text-gray-600 mb-4">
          Each point represents a page. Color indicates performance relative to expected CTR for that position.
        </p>
        
        <ResponsiveContainer width="100%" height={400}>
          <ScatterChart
            margin={{ top: 20, right: 20, bottom: 60, left: 60 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis
              type="number"
              dataKey="impressions"
              name="Impressions"
              label={{ 
                value: 'Impressions (Monthly)', 
                position: 'bottom',
                offset: 40,
                style: { fontSize: 12 }
              }}
              tickFormatter={formatNumber}
              stroke="#6b7280"
            />
            <YAxis
              type="number"
              dataKey="ctr"
              name="CTR"
              label={{ 
                value: 'Click-Through Rate (%)', 
                angle: -90, 
                position: 'left',
                offset: 40,
                style: { fontSize: 12 }
              }}
              tickFormatter={(value) => (value * 100).toFixed(1)}
              stroke="#6b7280"
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend 
              verticalAlign="top" 
              height={36}
              wrapperStyle={{ fontSize: 12 }}
            />
            
            <Scatter 
              name="Overperforming" 
              data={scatterData.filter(d => d.bucket === 'overperforming')} 
              fill={bucketColors.overperforming}
            />
            <Scatter 
              name="Expected" 
              data={scatterData.filter(d => d.bucket === 'expected')} 
              fill={bucketColors.expected}
            />
            <Scatter 
              name="Underperforming" 
              data={scatterData.filter(d => d.bucket === 'underperforming')} 
              fill={bucketColors.underperforming}
            />
            <Scatter 
              name="Critical" 
              data={scatterData.filter(d => d.bucket === 'critical')} 
              fill={bucketColors.critical}
            />
          </ScatterChart>
        </ResponsiveContainer>
      </div>

      {/* Top and Bottom Performers Tables */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Top Performers */}
        <div className="bg-white p-6 rounded-lg border border-gray-200">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
            <span className="text-green-600 mr-2">🏆</span>
            Top CTR Performers
          </h3>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left py-2 px-2 text-gray-600 font-medium">Page</th>
                  <th className="text-right py-2 px-2 text-gray-600 font-medium">CTR</th>
                  <th className="text-right py-2 px-2 text-gray-600 font-medium">Lift</th>
                </tr>
              </thead>
              <tbody>
                {data.ctr_analysis.top_performers.slice(0, 5).map((page, idx) => (
                  <tr key={idx} className="border-b border-gray-100 hover:bg-gray-50">
                    <td className="py-2 px-2">
                      <div className="truncate max-w-xs" title={page.url}>
                        {truncateUrl(page.url, 40)}
                      </div>
                      <div className="text-xs text-gray-500">
                        Pos: {page.position.toFixed(1)} • {page.impressions.toLocaleString()} imp
                      </div>
                    </td>
                    <td className="py-2 px-2 text-right">
                      <div className="font-medium text-green-600">
                        {(page.ctr * 100).toFixed(2)}%
                      </div>
                      <div className="text-xs text-gray-500">
                        vs {(page.expected_ctr * 100).toFixed(2)}%
                      </div>
                    </td>
                    <td className="py-2 px-2 text-right">
                      <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-green-100 text-green-800">
                        +{page.ctr_lift_pct.toFixed(0)}%
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Bottom Performers */}
        <div className="bg-white p-6 rounded-lg border border-gray-200">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
            <span className="text-red-600 mr-2">⚠️</span>
            Bottom CTR Performers
          </h3>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left py-2 px-2 text-gray-600 font-medium">Page</th>
                  <th className="text-right py-2 px-2 text-gray-600 font-medium">CTR</th>
                  <th className="text-right py-2 px-2 text-gray-600 font-medium">Gap</th>
                </tr>
              </thead>
              <tbody>
                {data.ctr_analysis.bottom_performers.slice(0, 5).map((page, idx) => (
                  <tr key={idx} className="border-b border-gray-100 hover:bg-gray-50">
                    <td className="py-2 px-2">
                      <div className="truncate max-w-xs" title={page.url}>
                        {truncateUrl(page.url, 40)}
                      </div>
                      <div className="text-xs text-gray-500">
                        Pos: {page.position.toFixed(1)} • {page.impressions.toLocaleString()} imp
                      </div>
                    </td>
                    <td className="py-2 px-2 text-right">
                      <div className="font-medium text-red-600">
                        {(page.ctr * 100).toFixed(2)}%
                      </div>
                      <div className="text-xs text-gray-500">
                        vs {(page.expected_ctr * 100).toFixed(2)}%
                      </div>
                    </td>
                    <td className="py-2 px-2 text-right">
                      <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-red-100 text-red-800">
                        {page.ctr_gap_pct.toFixed(0)}%
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* CTR Optimization Opportunities */}
      <div className="bg-white p-6 rounded-lg border border-gray-200">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">
          CTR Optimization Opportunities
        </h3>
        <p className="text-sm text-gray-600 mb-4">
          Pages with high impression volume and significant CTR improvement potential, 
          sorted by priority score (impact × feasibility).
        </p>
        
        <div className="space-y-3">
          {sortedOpportunities.slice(0, 10).map((opp, idx) => (
            <div 
              key={idx} 
              className="border border-gray-200 rounded-lg p-4 hover:border-blue-300 transition-colors"
            >
              <div className="flex items-start justify-between mb-2">
                <div className="flex-1 min-w-0 mr-4">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-lg">{getActionIcon(opp.action_type)}</span>
                    <h4 className="text-sm font-medium text-gray-900 truncate" title={opp.url}>
                      {truncateUrl(opp.url, 60)}
                    </h4>
                  </div>
                  <div className="flex items-center gap-3 text-xs text-gray-500 mb-2">
                    <span>{opp.impressions.toLocaleString()} impressions/mo</span>
                    <span>•</span>
                    <span>CTR: {(opp.ctr * 100).toFixed(2)}%</span>
                    <span>•</span>
                    <span>Expected: {(opp.expected_ctr * 100).toFixed(2)}%</span>
                  </div>
                </div>
                
                <div className="flex flex-col items-end gap-2">
                  <div className="text-right">
                    <div className="text-lg font-bold text-blue-600">
                      +{opp.estimated_click_gain.toLocaleString()}
                    </div>
                    <div className="text-xs text-gray-500">clicks/mo</div>
                  </div>
                  <span className={`inline-flex items-center px-2 py-1 rounded text-xs font-medium ${getEffortColor(opp.effort)}`}>
                    {opp.effort} effort
                  </span>
                </div>
              </div>
              
              <div className="bg-gray-50 rounded p-3 border border-gray-100">
                <div className="text-sm font-medium text-gray-700 mb-1">
                  Recommended Action:
                </div>
                <div className="text-sm text-gray-900">
                  {opp.recommended_action}
                </div>
              </div>
              
              <div className="mt-2 flex items-center justify-between">
                <div className="text-xs text-gray-500">
                  Priority Score: <span className="font-medium text-gray-700">{opp.priority_score.toFixed(1)}</span>
                </div>
                <div className="w-32 bg-gray-200 rounded-full h-2">
                  <div 
                    className="bg-blue-600 h-2 rounded-full"
                    style={{ width: `${Math.min(100, opp.priority_score)}%` }}
                  />
                </div>
              </div>
            </div>
          ))}
        </div>
        
        {sortedOpportunities.length > 10 && (
          <div className="mt-4 text-center">
            <button className="text-sm text-blue-600 hover:text-blue-700 font-medium">
              Show all {sortedOpportunities.length} opportunities →
            </button>
          </div>
        )}
      </div>

      {/* Action Type Summary */}
      <div className="bg-gradient-to-br from-blue-50 to-indigo-50 p-6 rounded-lg border border-blue-200">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">
          Quick CTR Wins Summary
        </h3>
        
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {['title_optimization', 'meta_description_optimization', 'schema_addition', 'content_snippet_optimization'].map(actionType => {
            const opportunities = sortedOpportunities.filter(o => o.action_type === actionType);
            const totalClicks = opportunities.reduce((sum, o) => sum + o.estimated_click_gain, 0);
            const lowEffort = opportunities.filter(o => o.effort === 'low').length;
            
            return (
              <div key={actionType} className="bg-white p-4 rounded-lg border border-gray-200">
                <div className="flex items-center gap-2 mb-2">
                  <span className="text-2xl">{getActionIcon(actionType)}</span>
                  <div className="text-sm font-medium text-gray-900">
                    {actionType.split('_').map(word => 
                      word.charAt(0).toUpperCase() + word.slice(1)
                    ).join(' ')}
                  </div>
                </div>
                <div className="text-2xl font-bold text-gray-900 mb-1">
                  {opportunities.length}
                </div>
                <div className="text-xs text-gray-600 mb-2">
                  +{totalClicks.toLocaleString()} clicks/mo potential
                </div>
                {lowEffort > 0 && (
                  <div className="text-xs">
                    <span className="inline-flex items-center px-2 py-1 rounded bg-green-100 text-green-800 font-medium">
                      {lowEffort} low effort
                    </span>
                  </div>
                )}
              </div>
            );
          })}
        </div>
        
        <div className="mt-4 p-4 bg-white rounded-lg border border-blue-200">
          <div className="flex items-start gap-3">
            <span className="text-2xl">💡</span>
            <div>
              <div className="text-sm font-medium text-gray-900 mb-1">
                Implementation Priority
              </div>
              <div className="text-sm text-gray-700">
                Start with the {sortedOpportunities.filter(o => o.effort === 'low').length} low-effort 
                opportunities above. These can typically be completed in 15-30 minutes per page and 
                collectively unlock <span className="font-semibold text-blue-600">
                  +{sortedOpportunities.filter(o => o.effort === 'low')
                    .reduce((sum, o) => sum + o.estimated_click_gain, 0).toLocaleString()} clicks/month
                </span>.
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Module5Chart;