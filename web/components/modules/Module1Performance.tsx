import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import { TrendingUp, TrendingDown, Minus, ArrowUp, ArrowDown } from 'lucide-react';

interface PerformanceData {
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
  time_series?: Array<{
    date: string;
    clicks: number;
    impressions: number;
    ctr: number;
    position: number;
  }>;
  summary_metrics?: {
    total_clicks: number;
    total_impressions: number;
    avg_ctr: number;
    avg_position: number;
    clicks_change_pct: number;
    impressions_change_pct: number;
    ctr_change_pct: number;
    position_change: number;
  };
}

interface Module1PerformanceProps {
  data: PerformanceData;
}

const Module1Performance: React.FC<Module1PerformanceProps> = ({ data }) => {
  const formatNumber = (num: number): string => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toFixed(0);
  };

  const formatPercent = (num: number): string => {
    return (num * 100).toFixed(2) + '%';
  };

  const formatPosition = (num: number): string => {
    return num.toFixed(1);
  };

  const getTrendIcon = (direction: string) => {
    switch (direction.toLowerCase()) {
      case 'strong_growth':
      case 'growth':
        return <TrendingUp className="w-5 h-5 text-green-600" />;
      case 'strong_decline':
      case 'decline':
        return <TrendingDown className="w-5 h-5 text-red-600" />;
      default:
        return <Minus className="w-5 h-5 text-gray-600" />;
    }
  };

  const getTrendColor = (direction: string): string => {
    switch (direction.toLowerCase()) {
      case 'strong_growth':
      case 'growth':
        return 'text-green-600';
      case 'strong_decline':
      case 'decline':
        return 'text-red-600';
      default:
        return 'text-gray-600';
    }
  };

  const getChangeColor = (value: number): string => {
    if (value > 0) return 'text-green-600';
    if (value < 0) return 'text-red-600';
    return 'text-gray-600';
  };

  const getDirectionLabel = (direction: string): string => {
    const labels: Record<string, string> = {
      strong_growth: 'Strong Growth',
      growth: 'Growth',
      flat: 'Flat',
      decline: 'Decline',
      strong_decline: 'Strong Decline',
      declining: 'Declining',
      stable: 'Stable',
      growing: 'Growing',
    };
    return labels[direction.toLowerCase()] || direction;
  };

  const formatDate = (dateString: string): string => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  };

  const formatChartDate = (dateString: string): string => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-4 border border-gray-200 rounded-lg shadow-lg">
          <p className="text-sm font-semibold text-gray-900 mb-2">{formatChartDate(label)}</p>
          {payload.map((entry: any, index: number) => (
            <div key={index} className="flex items-center justify-between gap-4 text-sm">
              <span className="flex items-center gap-2">
                <span
                  className="w-3 h-3 rounded-full"
                  style={{ backgroundColor: entry.color }}
                />
                {entry.name}:
              </span>
              <span className="font-semibold">
                {entry.name === 'CTR'
                  ? formatPercent(entry.value)
                  : entry.name === 'Position'
                  ? formatPosition(entry.value)
                  : formatNumber(entry.value)}
              </span>
            </div>
          ))}
        </div>
      );
    }
    return null;
  };

  const metrics = data.summary_metrics || {
    total_clicks: data.time_series?.reduce((sum, d) => sum + d.clicks, 0) || 0,
    total_impressions: data.time_series?.reduce((sum, d) => sum + d.impressions, 0) || 0,
    avg_ctr:
      data.time_series?.reduce((sum, d) => sum + d.ctr, 0) / (data.time_series?.length || 1) || 0,
    avg_position:
      data.time_series?.reduce((sum, d) => sum + d.position, 0) /
        (data.time_series?.length || 1) || 0,
    clicks_change_pct: data.trend_slope_pct_per_month || 0,
    impressions_change_pct: 0,
    ctr_change_pct: 0,
    position_change: 0,
  };

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-gray-900 mb-2">
          Module 1: Search Performance Overview
        </h2>
        <p className="text-gray-600">
          Comprehensive analysis of your search traffic trends, seasonality patterns, and
          performance trajectory
        </p>
      </div>

      {/* Key Metrics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        {/* Clicks Card */}
        <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-gray-600">Total Clicks</span>
            {metrics.clicks_change_pct !== 0 && (
              <span className={`text-xs flex items-center gap-1 ${getChangeColor(metrics.clicks_change_pct)}`}>
                {metrics.clicks_change_pct > 0 ? (
                  <ArrowUp className="w-3 h-3" />
                ) : (
                  <ArrowDown className="w-3 h-3" />
                )}
                {Math.abs(metrics.clicks_change_pct).toFixed(1)}%
              </span>
            )}
          </div>
          <div className="text-2xl font-bold text-gray-900">
            {formatNumber(metrics.total_clicks)}
          </div>
          <div className="text-xs text-gray-500 mt-1">Last 16 months</div>
        </div>

        {/* Impressions Card */}
        <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-gray-600">Total Impressions</span>
            {metrics.impressions_change_pct !== 0 && (
              <span className={`text-xs flex items-center gap-1 ${getChangeColor(metrics.impressions_change_pct)}`}>
                {metrics.impressions_change_pct > 0 ? (
                  <ArrowUp className="w-3 h-3" />
                ) : (
                  <ArrowDown className="w-3 h-3" />
                )}
                {Math.abs(metrics.impressions_change_pct).toFixed(1)}%
              </span>
            )}
          </div>
          <div className="text-2xl font-bold text-gray-900">
            {formatNumber(metrics.total_impressions)}
          </div>
          <div className="text-xs text-gray-500 mt-1">Last 16 months</div>
        </div>

        {/* CTR Card */}
        <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-gray-600">Average CTR</span>
            {metrics.ctr_change_pct !== 0 && (
              <span className={`text-xs flex items-center gap-1 ${getChangeColor(metrics.ctr_change_pct)}`}>
                {metrics.ctr_change_pct > 0 ? (
                  <ArrowUp className="w-3 h-3" />
                ) : (
                  <ArrowDown className="w-3 h-3" />
                )}
                {Math.abs(metrics.ctr_change_pct).toFixed(1)}%
              </span>
            )}
          </div>
          <div className="text-2xl font-bold text-gray-900">
            {formatPercent(metrics.avg_ctr)}
          </div>
          <div className="text-xs text-gray-500 mt-1">Last 16 months</div>
        </div>

        {/* Position Card */}
        <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-gray-600">Average Position</span>
            {metrics.position_change !== 0 && (
              <span className={`text-xs flex items-center gap-1 ${getChangeColor(-metrics.position_change)}`}>
                {metrics.position_change < 0 ? (
                  <ArrowUp className="w-3 h-3" />
                ) : (
                  <ArrowDown className="w-3 h-3" />
                )}
                {Math.abs(metrics.position_change).toFixed(1)}
              </span>
            )}
          </div>
          <div className="text-2xl font-bold text-gray-900">
            {formatPosition(metrics.avg_position)}
          </div>
          <div className="text-xs text-gray-500 mt-1">Last 16 months</div>
        </div>
      </div>

      {/* Performance Trend Chart */}
      {data.time_series && data.time_series.length > 0 && (
        <div className="mb-8">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Performance Trends</h3>
          <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
            <ResponsiveContainer width="100%" height={400}>
              <LineChart data={data.time_series}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis
                  dataKey="date"
                  tickFormatter={formatChartDate}
                  stroke="#6b7280"
                  style={{ fontSize: '12px' }}
                />
                <YAxis
                  yAxisId="left"
                  stroke="#6b7280"
                  style={{ fontSize: '12px' }}
                  tickFormatter={formatNumber}
                />
                <YAxis
                  yAxisId="right"
                  orientation="right"
                  stroke="#6b7280"
                  style={{ fontSize: '12px' }}
                  tickFormatter={(value) => formatPosition(value)}
                />
                <Tooltip content={<CustomTooltip />} />
                <Legend
                  wrapperStyle={{ fontSize: '14px' }}
                  iconType="circle"
                />
                <Line
                  yAxisId="left"
                  type="monotone"
                  dataKey="clicks"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  name="Clicks"
                  dot={false}
                />
                <Line
                  yAxisId="left"
                  type="monotone"
                  dataKey="impressions"
                  stroke="#8b5cf6"
                  strokeWidth={2}
                  name="Impressions"
                  dot={false}
                />
                <Line
                  yAxisId="right"
                  type="monotone"
                  dataKey="position"
                  stroke="#f59e0b"
                  strokeWidth={2}
                  name="Position"
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Overall Trajectory */}
      <div className="mb-8">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Overall Trajectory</h3>
        <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
          <div className="flex items-center gap-4 mb-4">
            {getTrendIcon(data.overall_direction)}
            <div>
              <div className="text-xl font-bold text-gray-900">
                {getDirectionLabel(data.overall_direction)}
              </div>
              <div className={`text-sm font-medium ${getTrendColor(data.overall_direction)}`}>
                {data.trend_slope_pct_per_month > 0 ? '+' : ''}
                {data.trend_slope_pct_per_month.toFixed(1)}% per month
              </div>
            </div>
          </div>
          <p className="text-gray-600">
            Your site is currently{' '}
            <span className={`font-semibold ${getTrendColor(data.overall_direction)}`}>
              {data.overall_direction.toLowerCase().replace('_', ' ')}
            </span>{' '}
            at a rate of{' '}
            <span className="font-semibold">
              {Math.abs(data.trend_slope_pct_per_month).toFixed(1)}% per month
            </span>
            . This trend has been extracted from your historical performance data using advanced
            statistical decomposition methods.
          </p>
        </div>
      </div>

      {/* Change Points */}
      {data.change_points && data.change_points.length > 0 && (
        <div className="mb-8">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Significant Changes</h3>
          <div className="space-y-3">
            {data.change_points.map((changePoint, index) => (
              <div
                key={index}
                className="bg-gray-50 rounded-lg p-4 border border-gray-200 flex items-center justify-between"
              >
                <div className="flex items-center gap-3">
                  {changePoint.direction === 'drop' ? (
                    <TrendingDown className="w-5 h-5 text-red-600" />
                  ) : (
                    <TrendingUp className="w-5 h-5 text-green-600" />
                  )}
                  <div>
                    <div className="font-semibold text-gray-900">
                      {formatDate(changePoint.date)}
                    </div>
                    <div className="text-sm text-gray-600">
                      Structural change detected:{' '}
                      <span
                        className={`font-medium ${
                          changePoint.direction === 'drop' ? 'text-red-600' : 'text-green-600'
                        }`}
                      >
                        {changePoint.direction === 'drop' ? 'Drop' : 'Increase'} of{' '}
                        {Math.abs(changePoint.magnitude * 100).toFixed(1)}%
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Seasonality Insights */}
      {data.seasonality && (
        <div className="mb-8">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Seasonality Patterns</h3>
          <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <div className="text-sm font-medium text-gray-600 mb-2">Weekly Pattern</div>
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-gray-700">Best day:</span>
                    <span className="font-semibold text-green-600">
                      {data.seasonality.best_day}
                    </span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-gray-700">Worst day:</span>
                    <span className="font-semibold text-red-600">
                      {data.seasonality.worst_day}
                    </span>
                  </div>
                </div>
              </div>
              {data.seasonality.monthly_cycle && (
                <div>
                  <div className="text-sm font-medium text-gray-600 mb-2">Monthly Pattern</div>
                  <p className="text-sm text-gray-700">{data.seasonality.cycle_description}</p>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Forecast */}
      {data.forecast && (
        <div className="mb-8">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Forecast</h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
              <div className="text-sm font-medium text-gray-600 mb-2">30 Days</div>
              <div className="text-2xl font-bold text-gray-900 mb-1">
                {formatNumber(data.forecast['30d'].clicks)}
              </div>
              <div className="text-xs text-gray-500">
                Range: {formatNumber(data.forecast['30d'].ci_low)} -{' '}
                {formatNumber(data.forecast['30d'].ci_high)}
              </div>
            </div>
            <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
              <div className="text-sm font-medium text-gray-600 mb-2">60 Days</div>
              <div className="text-2xl font-bold text-gray-900 mb-1">
                {formatNumber(data.forecast['60d'].clicks)}
              </div>
              <div className="text-xs text-gray-500">
                Range: {formatNumber(data.forecast['60d'].ci_low)} -{' '}
                {formatNumber(data.forecast['60d'].ci_high)}
              </div>
            </div>
            <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
              <div className="text-sm font-medium text-gray-600 mb-2">90 Days</div>
              <div className="text-2xl font-bold text-gray-900 mb-1">
                {formatNumber(data.forecast['90d'].clicks)}
              </div>
              <div className="text-xs text-gray-500">
                Range: {formatNumber(data.forecast['90d'].ci_low)} -{' '}
                {formatNumber(data.forecast['90d'].ci_high)}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Anomalies */}
      {data.anomalies && data.anomalies.length > 0 && (
        <div>
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Anomalies Detected</h3>
          <div className="space-y-3">
            {data.anomalies.map((anomaly, index) => (
              <div
                key={index}
                className="bg-yellow-50 rounded-lg p-4 border border-yellow-200 flex items-center justify-between"
              >
                <div>
                  <div className="font-semibold text-gray-900">{formatDate(anomaly.date)}</div>
                  <div className="text-sm text-gray-600">
                    <span className="capitalize">{anomaly.type}</span> detected with magnitude of{' '}
                    <span className="font-medium text-yellow-700">
                      {Math.abs(anomaly.magnitude * 100).toFixed(1)}%
                    </span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Key Insights Summary */}
      <div className="mt-8 pt-6 border-t border-gray-200">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Key Insights</h3>
        <div className="bg-blue-50 rounded-lg p-6 border border-blue-200">
          <ul className="space-y-3 text-sm text-gray-700">
            <li className="flex items-start gap-2">
              <span className="text-blue-600 mt-1">•</span>
              <span>
                Your search performance shows a{' '}
                <span className="font-semibold">
                  {getDirectionLabel(data.overall_direction).toLowerCase()}
                </span>{' '}
                trend with a monthly rate of change of{' '}
                <span className="font-semibold">
                  {data.trend_slope_pct_per_month > 0 ? '+' : ''}
                  {data.trend_slope_pct_per_month.toFixed(1)}%
                </span>
                .
              </span>
            </li>
            {data.seasonality.monthly_cycle && (
              <li className="flex items-start gap-2">
                <span className="text-blue-600 mt-1">•</span>
                <span>
                  A clear monthly cycle has been identified:{' '}
                  <span className="font-semibold">{data.seasonality.cycle_description}</span>
                </span>
              </li>
            )}
            {data.change_points && data.change_points.length > 0 && (
              <li className="flex items-start gap-2">
                <span className="text-blue-600 mt-1">•</span>
                <span>
                  <span className="font-semibold">{data.change_points.length}</span> significant
                  structural change{data.change_points.length > 1 ? 's' : ''} detected in your
                  performance history, with the most recent on{' '}
                  <span className="font-semibold">
                    {formatDate(data.change_points[0].date)}
                  </span>
                  .
                </span>
              </li>
            )}
            {data.forecast && (
              <li className="flex items-start gap-2">
                <span className="text-blue-600 mt-1">•</span>
                <span>
                  Based on current trends, your site is projected to receive approximately{' '}
                  <span className="font-semibold">
                    {formatNumber(data.forecast['30d'].clicks)} clicks
                  </span>{' '}
                  in the next 30 days.
                </span>
              </li>
            )}
            {data.anomalies && data.anomalies.length > 0 && (
              <li className="flex items-start gap-2">
                <span className="text-blue-600 mt-1">•</span>
                <span>
                  <span className="font-semibold">{data.anomalies.length}</span> unusual traffic
                  pattern{data.anomalies.length > 1 ? 's' : ''} detected that deviate from normal
                  behavior.
                </span>
              </li>
            )}
          </ul>
        </div>
      </div>
    </div>
  );
};

export default Module1Performance;