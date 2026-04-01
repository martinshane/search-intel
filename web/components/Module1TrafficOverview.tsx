import React from 'react';
import { ArrowUp, ArrowDown, TrendingUp, TrendingDown, Activity, Eye, MousePointerClick, BarChart3 } from 'lucide-react';
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

interface Module1Data {
  overall_direction: 'strong_growth' | 'growth' | 'flat' | 'decline' | 'strong_decline';
  trend_slope_pct_per_month: number;
  change_points: Array<{
    date: string;
    magnitude: number;
    direction: 'spike' | 'drop';
  }>;
  seasonality: {
    best_day: string;
    worst_day: string;
    monthly_cycle: boolean;
    cycle_description: string;
  };
  anomalies: Array<{
    date: string;
    type: 'motif' | 'discord';
    magnitude: number;
  }>;
  forecast: {
    '30d': { clicks: number; ci_low: number; ci_high: number };
    '60d': { clicks: number; ci_low: number; ci_high: number };
    '90d': { clicks: number; ci_low: number; ci_high: number };
  };
  time_series: Array<{
    date: string;
    clicks: number;
    impressions: number;
    sessions: number;
  }>;
  summary_stats: {
    current_monthly_clicks: number;
    current_monthly_impressions: number;
    current_monthly_sessions: number;
    clicks_change_pct: number;
    impressions_change_pct: number;
    sessions_change_pct: number;
  };
}

interface Module1TrafficOverviewProps {
  data: Module1Data | null;
  loading: boolean;
  error: string | null;
}

const Module1TrafficOverview: React.FC<Module1TrafficOverviewProps> = ({ data, loading, error }) => {
  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-200 rounded w-1/3 mb-4"></div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
            {[1, 2, 3].map((i) => (
              <div key={i} className="h-32 bg-gray-200 rounded"></div>
            ))}
          </div>
          <div className="h-96 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="flex items-center gap-3 text-red-600 mb-4">
          <Activity className="w-6 h-6" />
          <h2 className="text-2xl font-bold">Health & Trajectory</h2>
        </div>
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <p className="text-red-800">
            <strong>Error loading module data:</strong> {error}
          </p>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="flex items-center gap-3 text-gray-600 mb-4">
          <Activity className="w-6 h-6" />
          <h2 className="text-2xl font-bold">Health & Trajectory</h2>
        </div>
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
          <p className="text-gray-600">No data available.</p>
        </div>
      </div>
    );
  }

  const getDirectionColor = (direction: string): string => {
    switch (direction) {
      case 'strong_growth':
        return 'text-green-600';
      case 'growth':
        return 'text-green-500';
      case 'flat':
        return 'text-gray-600';
      case 'decline':
        return 'text-orange-500';
      case 'strong_decline':
        return 'text-red-600';
      default:
        return 'text-gray-600';
    }
  };

  const getDirectionLabel = (direction: string): string => {
    switch (direction) {
      case 'strong_growth':
        return 'Strong Growth';
      case 'growth':
        return 'Growth';
      case 'flat':
        return 'Flat';
      case 'decline':
        return 'Decline';
      case 'strong_decline':
        return 'Strong Decline';
      default:
        return direction;
    }
  };

  const getDirectionIcon = (direction: string) => {
    switch (direction) {
      case 'strong_growth':
      case 'growth':
        return <TrendingUp className="w-5 h-5" />;
      case 'decline':
      case 'strong_decline':
        return <TrendingDown className="w-5 h-5" />;
      default:
        return <Activity className="w-5 h-5" />;
    }
  };

  const formatNumber = (num: number): string => {
    return new Intl.NumberFormat('en-US').format(Math.round(num));
  };

  const formatPercentage = (num: number): string => {
    const sign = num >= 0 ? '+' : '';
    return `${sign}${num.toFixed(1)}%`;
  };

  const MetricCard: React.FC<{
    title: string;
    value: number;
    change: number;
    icon: React.ReactNode;
  }> = ({ title, value, change }) => {
    const isPositive = change >= 0;
    const changeColor = isPositive ? 'text-green-600' : 'text-red-600';
    const ArrowIcon = isPositive ? ArrowUp : ArrowDown;

    return (
      <div className="bg-gradient-to-br from-gray-50 to-white border border-gray-200 rounded-lg p-5 hover:shadow-md transition-shadow">
        <div className="flex items-center justify-between mb-3">
          <span className="text-sm font-medium text-gray-600">{title}</span>
          <div className={`flex items-center gap-1 ${changeColor} text-sm font-semibold`}>
            <ArrowIcon className="w-4 h-4" />
            <span>{formatPercentage(change)}</span>
          </div>
        </div>
        <div className="text-3xl font-bold text-gray-900">{formatNumber(value)}</div>
        <div className="mt-2 text-xs text-gray-500">Last 30 days</div>
      </div>
    );
  };

  const CustomTooltip: React.FC<any> = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white border border-gray-300 rounded-lg shadow-lg p-4">
          <p className="font-semibold text-gray-900 mb-2">{label}</p>
          {payload.map((entry: any, index: number) => (
            <div key={index} className="flex items-center gap-2 text-sm">
              <div
                className="w-3 h-3 rounded-full"
                style={{ backgroundColor: entry.color }}
              ></div>
              <span className="text-gray-600">{entry.name}:</span>
              <span className="font-semibold text-gray-900">
                {formatNumber(entry.value)}
              </span>
            </div>
          ))}
        </div>
      );
    }
    return null;
  };

  const chartData = data.time_series.map((item) => ({
    date: new Date(item.date).toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
    }),
    Clicks: item.clicks,
    Sessions: item.sessions,
  }));

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center gap-3 mb-2">
          <Activity className="w-6 h-6 text-blue-600" />
          <h2 className="text-2xl font-bold text-gray-900">Health & Trajectory</h2>
        </div>
        <div className="flex items-center gap-3 mt-3">
          <span
            className={`inline-flex items-center gap-2 px-3 py-1 rounded-full text-sm font-semibold ${getDirectionColor(
              data.overall_direction
            )} bg-opacity-10`}
            style={{
              backgroundColor:
                data.overall_direction === 'strong_growth' || data.overall_direction === 'growth'
                  ? 'rgba(34, 197, 94, 0.1)'
                  : data.overall_direction === 'decline' ||
                    data.overall_direction === 'strong_decline'
                  ? 'rgba(239, 68, 68, 0.1)'
                  : 'rgba(107, 114, 128, 0.1)',
            }}
          >
            {getDirectionIcon(data.overall_direction)}
            {getDirectionLabel(data.overall_direction)}
          </span>
          <span className="text-gray-600">
            {formatPercentage(data.trend_slope_pct_per_month)} per month
          </span>
        </div>
      </div>

      {/* Metric Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        <MetricCard
          title="Monthly Clicks"
          value={data.summary_stats.current_monthly_clicks}
          change={data.summary_stats.clicks_change_pct}
          icon={<MousePointerClick className="w-5 h-5 text-blue-600" />}
        />
        <MetricCard
          title="Monthly Impressions"
          value={data.summary_stats.current_monthly_impressions}
          change={data.summary_stats.impressions_change_pct}
          icon={<Eye className="w-5 h-5 text-purple-600" />}
        />
        <MetricCard
          title="Monthly Sessions"
          value={data.summary_stats.current_monthly_sessions}
          change={data.summary_stats.sessions_change_pct}
          icon={<BarChart3 className="w-5 h-5 text-green-600" />}
        />
      </div>

      {/* Time Series Chart */}
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">90-Day Traffic Trend</h3>
        <div className="bg-gray-50 rounded-lg p-4" style={{ height: '400px' }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="date"
                tick={{ fill: '#6b7280', fontSize: 12 }}
                tickLine={{ stroke: '#e5e7eb' }}
              />
              <YAxis
                yAxisId="left"
                tick={{ fill: '#6b7280', fontSize: 12 }}
                tickLine={{ stroke: '#e5e7eb' }}
                label={{
                  value: 'Clicks',
                  angle: -90,
                  position: 'insideLeft',
                  style: { fill: '#6b7280', fontSize: 12 },
                }}
              />
              <YAxis
                yAxisId="right"
                orientation="right"
                tick={{ fill: '#6b7280', fontSize: 12 }}
                tickLine={{ stroke: '#e5e7eb' }}
                label={{
                  value: 'Sessions',
                  angle: 90,
                  position: 'insideRight',
                  style: { fill: '#6b7280', fontSize: 12 },
                }}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend
                wrapperStyle={{ paddingTop: '20px' }}
                iconType="line"
                formatter={(value) => <span className="text-sm font-medium">{value}</span>}
              />
              <Line
                yAxisId="left"
                type="monotone"
                dataKey="Clicks"
                stroke="#3b82f6"
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 6 }}
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="Sessions"
                stroke="#10b981"
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 6 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Seasonality & Forecast */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Seasonality Card */}
        <div className="bg-gradient-to-br from-blue-50 to-white border border-blue-200 rounded-lg p-5">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Seasonality Pattern</h3>
          <div className="space-y-3">
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-600">Best day:</span>
              <span className="font-semibold text-green-700">{data.seasonality.best_day}</span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-600">Worst day:</span>
              <span className="font-semibold text-red-700">{data.seasonality.worst_day}</span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-600">Monthly cycle:</span>
              <span className="font-semibold text-gray-900">
                {data.seasonality.monthly_cycle ? 'Yes' : 'No'}
              </span>
            </div>
            {data.seasonality.cycle_description && (
              <div className="mt-4 pt-3 border-t border-blue-200">
                <p className="text-sm text-gray-700">{data.seasonality.cycle_description}</p>
              </div>
            )}
          </div>
        </div>

        {/* Forecast Card */}
        <div className="bg-gradient-to-br from-purple-50 to-white border border-purple-200 rounded-lg p-5">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Forecast</h3>
          <div className="space-y-4">
            {Object.entries(data.forecast).map(([period, forecast]) => (
              <div key={period} className="flex flex-col">
                <div className="flex justify-between items-center mb-1">
                  <span className="text-sm font-medium text-gray-600">
                    {period === '30d' ? '30 Days' : period === '60d' ? '60 Days' : '90 Days'}
                  </span>
                  <span className="text-lg font-bold text-gray-900">
                    {formatNumber(forecast.clicks)}
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="flex-1 bg-gray-200 rounded-full h-2 overflow-hidden">
                    <div
                      className="bg-purple-600 h-2 rounded-full"
                      style={{
                        width: `${
                          ((forecast.clicks - forecast.ci_low) /
                            (forecast.ci_high - forecast.ci_low)) *
                          100
                        }%`,
                      }}
                    ></div>
                  </div>
                  <span className="text-xs text-gray-500 whitespace-nowrap">
                    {formatNumber(forecast.ci_low)} - {formatNumber(forecast.ci_high)}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Change Points & Anomalies */}
      {(data.change_points.length > 0 || data.anomalies.length > 0) && (
        <div className="mt-6 grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Change Points */}
          {data.change_points.length > 0 && (
            <div className="bg-gradient-to-br from-orange-50 to-white border border-orange-200 rounded-lg p-5">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Change Points</h3>
              <div className="space-y-3">
                {data.change_points.slice(0, 3).map((point, index) => (
                  <div
                    key={index}
                    className="flex items-center justify-between p-3 bg-white rounded border border-orange-100"
                  >
                    <div className="flex items-center gap-3">
                      {point.direction === 'spike' ? (
                        <TrendingUp className="w-4 h-4 text-green-600" />
                      ) : (
                        <TrendingDown className="w-4 h-4 text-red-600" />
                      )}
                      <span className="text-sm font-medium text-gray-900">
                        {new Date(point.date).toLocaleDateString('en-US', {
                          month: 'short',
                          day: 'numeric',
                          year: 'numeric',
                        })}
                      </span>
                    </div>
                    <span
                      className={`text-sm font-semibold ${
                        point.magnitude >= 0 ? 'text-green-600' : 'text-red-600'
                      }`}
                    >
                      {formatPercentage(point.magnitude * 100)}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Anomalies */}
          {data.anomalies.length > 0 && (
            <div className="bg-gradient-to-br from-yellow-50 to-white border border-yellow-200 rounded-lg p-5">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Anomalies Detected</h3>
              <div className="space-y-3">
                {data.anomalies.slice(0, 3).map((anomaly, index) => (
                  <div
                    key={index}
                    className="flex items-center justify-between p-3 bg-white rounded border border-yellow-100"
                  >
                    <div className="flex items-center gap-3">
                      <Activity className="w-4 h-4 text-yellow-600" />
                      <span className="text-sm font-medium text-gray-900">
                        {new Date(anomaly.date).toLocaleDateString('en-US', {
                          month: 'short',
                          day: 'numeric',
                          year: 'numeric',
                        })}
                      </span>
                      <span className="text-xs text-gray-500 capitalize">
                        ({anomaly.type})
                      </span>
                    </div>
                    <span className="text-sm font-semibold text-gray-700">
                      {formatPercentage(anomaly.magnitude * 100)}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default Module1TrafficOverview;
