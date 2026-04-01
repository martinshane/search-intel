import React from 'react';
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import { TrendingUp, TrendingDown, Minus, AlertCircle } from 'lucide-react';

interface TimeSeriesDataPoint {
  date: string;
  clicks: number;
  impressions: number;
  ctr: number;
  position: number;
}

interface Forecast {
  clicks: number;
  ci_low: number;
  ci_high: number;
}

interface ChangePoint {
  date: string;
  magnitude: number;
  direction: 'drop' | 'spike';
}

interface Seasonality {
  best_day: string;
  worst_day: string;
  monthly_cycle: boolean;
  cycle_description: string;
}

interface Anomaly {
  date: string;
  type: 'discord' | 'motif';
  magnitude: number;
}

interface Module1Data {
  overall_direction: 'strong_growth' | 'growth' | 'flat' | 'decline' | 'strong_decline';
  trend_slope_pct_per_month: number;
  change_points: ChangePoint[];
  seasonality: Seasonality;
  anomalies: Anomaly[];
  forecast: {
    '30d': Forecast;
    '60d': Forecast;
    '90d': Forecast;
  };
  time_series: TimeSeriesDataPoint[];
}

interface Module1TrafficOverviewProps {
  module1?: Module1Data;
  loading?: boolean;
  error?: string;
}

const Module1TrafficOverview: React.FC<Module1TrafficOverviewProps> = ({
  module1,
  loading = false,
  error,
}) => {
  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-200 rounded w-1/3 mb-6"></div>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="h-24 bg-gray-200 rounded"></div>
            ))}
          </div>
          <div className="h-96 bg-gray-200 rounded mb-6"></div>
          <div className="h-64 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="flex items-center justify-center text-red-600 py-12">
          <AlertCircle className="w-6 h-6 mr-2" />
          <span className="text-lg">{error}</span>
        </div>
      </div>
    );
  }

  if (!module1) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="flex items-center justify-center text-gray-500 py-12">
          <AlertCircle className="w-6 h-6 mr-2" />
          <span className="text-lg">No data available</span>
        </div>
      </div>
    );
  }

  const getTrendIcon = (direction: string) => {
    switch (direction) {
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

  const getTrendColor = (direction: string) => {
    switch (direction) {
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

  const getTrendLabel = (direction: string) => {
    const labels: Record<string, string> = {
      strong_growth: 'Strong Growth',
      growth: 'Growth',
      flat: 'Flat',
      decline: 'Decline',
      strong_decline: 'Strong Decline',
    };
    return labels[direction] || direction;
  };

  const formatNumber = (num: number): string => {
    if (num >= 1000000) {
      return `${(num / 1000000).toFixed(2)}M`;
    }
    if (num >= 1000) {
      return `${(num / 1000).toFixed(1)}K`;
    }
    return num.toFixed(0);
  };

  const formatPercentage = (num: number): string => {
    return `${(num * 100).toFixed(2)}%`;
  };

  const formatPosition = (num: number): string => {
    return num.toFixed(1);
  };

  const calculateCurrentMetrics = () => {
    if (!module1.time_series || module1.time_series.length === 0) {
      return {
        clicks: 0,
        impressions: 0,
        ctr: 0,
        position: 0,
      };
    }

    const last30Days = module1.time_series.slice(-30);
    const totalClicks = last30Days.reduce((sum, d) => sum + d.clicks, 0);
    const totalImpressions = last30Days.reduce((sum, d) => sum + d.impressions, 0);
    const avgCtr = totalImpressions > 0 ? totalClicks / totalImpressions : 0;
    const avgPosition =
      last30Days.reduce((sum, d) => sum + d.position, 0) / last30Days.length;

    return {
      clicks: totalClicks,
      impressions: totalImpressions,
      ctr: avgCtr,
      position: avgPosition,
    };
  };

  const currentMetrics = calculateCurrentMetrics();

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-4 rounded-lg shadow-lg border border-gray-200">
          <p className="font-semibold mb-2">{label}</p>
          {payload.map((entry: any, index: number) => (
            <p key={index} className="text-sm" style={{ color: entry.color }}>
              {entry.name}: {entry.name === 'CTR' ? formatPercentage(entry.value) : formatNumber(entry.value)}
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      {/* Header */}
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-gray-900 mb-2">
          Health & Trajectory Overview
        </h2>
        <div className="flex items-center space-x-3">
          {getTrendIcon(module1.overall_direction)}
          <span className={`text-lg font-semibold ${getTrendColor(module1.overall_direction)}`}>
            {getTrendLabel(module1.overall_direction)}
          </span>
          <span className="text-gray-600">
            ({module1.trend_slope_pct_per_month > 0 ? '+' : ''}
            {module1.trend_slope_pct_per_month.toFixed(1)}% per month)
          </span>
        </div>
      </div>

      {/* Metric Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div className="bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg p-4">
          <div className="text-sm text-blue-700 font-medium mb-1">Total Clicks</div>
          <div className="text-2xl font-bold text-blue-900">
            {formatNumber(currentMetrics.clicks)}
          </div>
          <div className="text-xs text-blue-600 mt-1">Last 30 days</div>
        </div>

        <div className="bg-gradient-to-br from-purple-50 to-purple-100 rounded-lg p-4">
          <div className="text-sm text-purple-700 font-medium mb-1">Impressions</div>
          <div className="text-2xl font-bold text-purple-900">
            {formatNumber(currentMetrics.impressions)}
          </div>
          <div className="text-xs text-purple-600 mt-1">Last 30 days</div>
        </div>

        <div className="bg-gradient-to-br from-green-50 to-green-100 rounded-lg p-4">
          <div className="text-sm text-green-700 font-medium mb-1">Avg CTR</div>
          <div className="text-2xl font-bold text-green-900">
            {formatPercentage(currentMetrics.ctr)}
          </div>
          <div className="text-xs text-green-600 mt-1">Last 30 days</div>
        </div>

        <div className="bg-gradient-to-br from-orange-50 to-orange-100 rounded-lg p-4">
          <div className="text-sm text-orange-700 font-medium mb-1">Avg Position</div>
          <div className="text-2xl font-bold text-orange-900">
            {formatPosition(currentMetrics.position)}
          </div>
          <div className="text-xs text-orange-600 mt-1">Last 30 days</div>
        </div>
      </div>

      {/* Main Chart - Clicks and Impressions */}
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Trend</h3>
        <ResponsiveContainer width="100%" height={400}>
          <AreaChart data={module1.time_series}>
            <defs>
              <linearGradient id="colorClicks" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.8} />
                <stop offset="95%" stopColor="#3b82f6" stopOpacity={0.1} />
              </linearGradient>
              <linearGradient id="colorImpressions" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.8} />
                <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0.1} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis
              dataKey="date"
              tickFormatter={formatDate}
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
            />
            <YAxis stroke="#6b7280" style={{ fontSize: '12px' }} />
            <Tooltip content={<CustomTooltip />} />
            <Legend />
            <Area
              type="monotone"
              dataKey="clicks"
              stroke="#3b82f6"
              fillOpacity={1}
              fill="url(#colorClicks)"
              name="Clicks"
            />
            <Area
              type="monotone"
              dataKey="impressions"
              stroke="#8b5cf6"
              fillOpacity={1}
              fill="url(#colorImpressions)"
              name="Impressions"
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* CTR and Position Chart */}
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">CTR & Position</h3>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={module1.time_series}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis
              dataKey="date"
              tickFormatter={formatDate}
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
            />
            <YAxis yAxisId="left" stroke="#6b7280" style={{ fontSize: '12px' }} />
            <YAxis
              yAxisId="right"
              orientation="right"
              reversed
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="ctr"
              stroke="#10b981"
              strokeWidth={2}
              dot={false}
              name="CTR"
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="position"
              stroke="#f59e0b"
              strokeWidth={2}
              dot={false}
              name="Position"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Forecast */}
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Forecast</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {['30d', '60d', '90d'].map((period) => {
            const forecast = module1.forecast[period as keyof typeof module1.forecast];
            return (
              <div key={period} className="bg-gray-50 rounded-lg p-4 border border-gray-200">
                <div className="text-sm text-gray-600 font-medium mb-2">
                  {period === '30d' ? '30 Days' : period === '60d' ? '60 Days' : '90 Days'}
                </div>
                <div className="text-2xl font-bold text-gray-900 mb-1">
                  {formatNumber(forecast.clicks)}
                </div>
                <div className="text-xs text-gray-500">
                  Range: {formatNumber(forecast.ci_low)} - {formatNumber(forecast.ci_high)}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Seasonality Insights */}
      {module1.seasonality && (
        <div className="mb-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Seasonality Patterns</h3>
          <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-3">
              <div>
                <span className="text-sm text-blue-700 font-medium">Best Day:</span>
                <span className="ml-2 text-sm text-blue-900 font-semibold">
                  {module1.seasonality.best_day}
                </span>
              </div>
              <div>
                <span className="text-sm text-blue-700 font-medium">Worst Day:</span>
                <span className="ml-2 text-sm text-blue-900 font-semibold">
                  {module1.seasonality.worst_day}
                </span>
              </div>
            </div>
            {module1.seasonality.monthly_cycle && (
              <div className="text-sm text-blue-800">
                <span className="font-medium">Monthly Pattern:</span>
                <span className="ml-2">{module1.seasonality.cycle_description}</span>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Change Points */}
      {module1.change_points && module1.change_points.length > 0 && (
        <div className="mb-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">
            Significant Changes Detected
          </h3>
          <div className="space-y-2">
            {module1.change_points.map((cp, index) => (
              <div
                key={index}
                className={`flex items-center justify-between p-3 rounded-lg border ${
                  cp.direction === 'drop'
                    ? 'bg-red-50 border-red-200'
                    : 'bg-green-50 border-green-200'
                }`}
              >
                <div className="flex items-center space-x-3">
                  {cp.direction === 'drop' ? (
                    <TrendingDown className="w-5 h-5 text-red-600" />
                  ) : (
                    <TrendingUp className="w-5 h-5 text-green-600" />
                  )}
                  <div>
                    <div className="text-sm font-medium text-gray-900">
                      {new Date(cp.date).toLocaleDateString('en-US', {
                        year: 'numeric',
                        month: 'long',
                        day: 'numeric',
                      })}
                    </div>
                    <div
                      className={`text-xs ${
                        cp.direction === 'drop' ? 'text-red-600' : 'text-green-600'
                      }`}
                    >
                      {cp.direction === 'drop' ? 'Traffic Drop' : 'Traffic Spike'}
                    </div>
                  </div>
                </div>
                <div
                  className={`text-sm font-bold ${
                    cp.direction === 'drop' ? 'text-red-700' : 'text-green-700'
                  }`}
                >
                  {cp.magnitude > 0 ? '+' : ''}
                  {(cp.magnitude * 100).toFixed(1)}%
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Anomalies */}
      {module1.anomalies && module1.anomalies.length > 0 && (
        <div>
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Notable Anomalies</h3>
          <div className="space-y-2">
            {module1.anomalies.slice(0, 5).map((anomaly, index) => (
              <div
                key={index}
                className="flex items-center justify-between p-3 bg-yellow-50 rounded-lg border border-yellow-200"
              >
                <div className="flex items-center space-x-3">
                  <AlertCircle className="w-5 h-5 text-yellow-600" />
                  <div>
                    <div className="text-sm font-medium text-gray-900">
                      {new Date(anomaly.date).toLocaleDateString('en-US', {
                        year: 'numeric',
                        month: 'long',
                        day: 'numeric',
                      })}
                    </div>
                    <div className="text-xs text-yellow-600">
                      {anomaly.type === 'discord' ? 'One-time event' : 'Recurring pattern'}
                    </div>
                  </div>
                </div>
                <div className="text-sm font-bold text-yellow-700">
                  {(anomaly.magnitude * 100).toFixed(1)}% deviation
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default Module1TrafficOverview;
