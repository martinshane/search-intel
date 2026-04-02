import React from 'react';
import { ArrowUp, ArrowDown, TrendingUp, TrendingDown, Activity, Eye, MousePointerClick, BarChart3, Users, Monitor, Smartphone, Tablet } from 'lucide-react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
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
    users: number;
  }>;
  summary_stats: {
    current_monthly_clicks: number;
    current_monthly_impressions: number;
    current_monthly_sessions: number;
    current_monthly_users: number;
    clicks_change_pct: number;
    impressions_change_pct: number;
    sessions_change_pct: number;
    users_change_pct: number;
  };
  device_breakdown: Array<{
    device: string;
    sessions: number;
    users: number;
    percentage: number;
  }>;
  top_pages: Array<{
    url: string;
    sessions: number;
    users: number;
    bounce_rate: number;
    avg_session_duration: number;
  }>;
}

interface Module1TrafficOverviewProps {
  data: Module1Data | null;
  loading: boolean;
  error: string | null;
}

const DEVICE_COLORS = {
  desktop: '#3b82f6',
  mobile: '#10b981',
  tablet: '#f59e0b',
};

const Module1TrafficOverview: React.FC<Module1TrafficOverviewProps> = ({ data, loading, error }) => {
  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-200 rounded w-1/3 mb-6"></div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
            {[1, 2, 3, 4].map((i) => (
              <div key={i} className="h-32 bg-gray-200 rounded"></div>
            ))}
          </div>
          <div className="h-96 bg-gray-200 rounded mb-6"></div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
            <div className="h-80 bg-gray-200 rounded"></div>
            <div className="h-80 bg-gray-200 rounded"></div>
          </div>
          <div className="h-64 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="flex items-center gap-3 text-red-600 mb-4">
          <Activity className="w-6 h-6" />
          <h2 className="text-2xl font-bold">Traffic Overview & Health</h2>
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
          <h2 className="text-2xl font-bold">Traffic Overview & Health</h2>
        </div>
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
          <p className="text-gray-600">No data available.</p>
        </div>
      </div>
    );
  }

  const getDirectionColor = (direction: string) => {
    switch (direction) {
      case 'strong_growth':
        return 'text-green-600 bg-green-50';
      case 'growth':
        return 'text-green-500 bg-green-50';
      case 'flat':
        return 'text-gray-600 bg-gray-50';
      case 'decline':
        return 'text-orange-500 bg-orange-50';
      case 'strong_decline':
        return 'text-red-600 bg-red-50';
      default:
        return 'text-gray-600 bg-gray-50';
    }
  };

  const getDirectionIcon = (direction: string) => {
    if (direction.includes('growth')) {
      return <TrendingUp className="w-5 h-5" />;
    } else if (direction.includes('decline')) {
      return <TrendingDown className="w-5 h-5" />;
    }
    return <Activity className="w-5 h-5" />;
  };

  const getDirectionLabel = (direction: string) => {
    return direction
      .split('_')
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
  };

  const formatNumber = (num: number): string => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toFixed(0);
  };

  const formatPercent = (num: number): string => {
    const sign = num > 0 ? '+' : '';
    return `${sign}${num.toFixed(1)}%`;
  };

  const getChangeIcon = (change: number) => {
    if (change > 0) {
      return <ArrowUp className="w-4 h-4" />;
    } else if (change < 0) {
      return <ArrowDown className="w-4 h-4" />;
    }
    return null;
  };

  const getChangeColor = (change: number) => {
    if (change > 0) return 'text-green-600';
    if (change < 0) return 'text-red-600';
    return 'text-gray-600';
  };

  // Prepare time series data for chart
  const chartData = data.time_series.map((item) => ({
    date: new Date(item.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
    sessions: item.sessions,
    users: item.users,
  }));

  // Prepare device breakdown data for pie chart
  const deviceData = data.device_breakdown.map((item) => ({
    name: item.device.charAt(0).toUpperCase() + item.device.slice(1),
    value: item.sessions,
    percentage: item.percentage,
  }));

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <Activity className="w-6 h-6 text-blue-600" />
        <h2 className="text-2xl font-bold text-gray-900">Traffic Overview & Health</h2>
      </div>

      {/* Overall Direction Badge */}
      <div className="mb-6">
        <div
          className={`inline-flex items-center gap-2 px-4 py-2 rounded-full font-semibold ${getDirectionColor(
            data.overall_direction
          )}`}
        >
          {getDirectionIcon(data.overall_direction)}
          <span>{getDirectionLabel(data.overall_direction)}</span>
          <span className="ml-1">
            ({formatPercent(data.trend_slope_pct_per_month)}/month)
          </span>
        </div>
      </div>

      {/* Key Metrics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        {/* Total Sessions */}
        <div className="bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg p-5 border border-blue-200">
          <div className="flex items-center justify-between mb-2">
            <div className="p-2 bg-blue-600 rounded-lg">
              <MousePointerClick className="w-5 h-5 text-white" />
            </div>
            {data.summary_stats.sessions_change_pct !== 0 && (
              <div
                className={`flex items-center gap-1 text-sm font-semibold ${getChangeColor(
                  data.summary_stats.sessions_change_pct
                )}`}
              >
                {getChangeIcon(data.summary_stats.sessions_change_pct)}
                {formatPercent(Math.abs(data.summary_stats.sessions_change_pct))}
              </div>
            )}
          </div>
          <div className="text-2xl font-bold text-gray-900 mb-1">
            {formatNumber(data.summary_stats.current_monthly_sessions)}
          </div>
          <div className="text-sm text-gray-600 font-medium">Total Sessions</div>
        </div>

        {/* Total Users */}
        <div className="bg-gradient-to-br from-green-50 to-green-100 rounded-lg p-5 border border-green-200">
          <div className="flex items-center justify-between mb-2">
            <div className="p-2 bg-green-600 rounded-lg">
              <Users className="w-5 h-5 text-white" />
            </div>
            {data.summary_stats.users_change_pct !== 0 && (
              <div
                className={`flex items-center gap-1 text-sm font-semibold ${getChangeColor(
                  data.summary_stats.users_change_pct
                )}`}
              >
                {getChangeIcon(data.summary_stats.users_change_pct)}
                {formatPercent(Math.abs(data.summary_stats.users_change_pct))}
              </div>
            )}
          </div>
          <div className="text-2xl font-bold text-gray-900 mb-1">
            {formatNumber(data.summary_stats.current_monthly_users)}
          </div>
          <div className="text-sm text-gray-600 font-medium">Total Users</div>
        </div>

        {/* Total Clicks */}
        <div className="bg-gradient-to-br from-purple-50 to-purple-100 rounded-lg p-5 border border-purple-200">
          <div className="flex items-center justify-between mb-2">
            <div className="p-2 bg-purple-600 rounded-lg">
              <BarChart3 className="w-5 h-5 text-white" />
            </div>
            {data.summary_stats.clicks_change_pct !== 0 && (
              <div
                className={`flex items-center gap-1 text-sm font-semibold ${getChangeColor(
                  data.summary_stats.clicks_change_pct
                )}`}
              >
                {getChangeIcon(data.summary_stats.clicks_change_pct)}
                {formatPercent(Math.abs(data.summary_stats.clicks_change_pct))}
              </div>
            )}
          </div>
          <div className="text-2xl font-bold text-gray-900 mb-1">
            {formatNumber(data.summary_stats.current_monthly_clicks)}
          </div>
          <div className="text-sm text-gray-600 font-medium">Search Clicks</div>
        </div>

        {/* Total Impressions */}
        <div className="bg-gradient-to-br from-orange-50 to-orange-100 rounded-lg p-5 border border-orange-200">
          <div className="flex items-center justify-between mb-2">
            <div className="p-2 bg-orange-600 rounded-lg">
              <Eye className="w-5 h-5 text-white" />
            </div>
            {data.summary_stats.impressions_change_pct !== 0 && (
              <div
                className={`flex items-center gap-1 text-sm font-semibold ${getChangeColor(
                  data.summary_stats.impressions_change_pct
                )}`}
              >
                {getChangeIcon(data.summary_stats.impressions_change_pct)}
                {formatPercent(Math.abs(data.summary_stats.impressions_change_pct))}
              </div>
            )}
          </div>
          <div className="text-2xl font-bold text-gray-900 mb-1">
            {formatNumber(data.summary_stats.current_monthly_impressions)}
          </div>
          <div className="text-sm text-gray-600 font-medium">Search Impressions</div>
        </div>
      </div>

      {/* Traffic Trend Chart */}
      <div className="bg-gray-50 rounded-lg p-6 mb-8">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Trend Over Time</h3>
        <ResponsiveContainer width="100%" height={400}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis
              dataKey="date"
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
              tick={{ fill: '#6b7280' }}
            />
            <YAxis
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
              tick={{ fill: '#6b7280' }}
              tickFormatter={formatNumber}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: '#fff',
                border: '1px solid #e5e7eb',
                borderRadius: '8px',
                padding: '12px',
              }}
              formatter={(value: number) => [formatNumber(value), '']}
              labelStyle={{ color: '#111827', fontWeight: 600, marginBottom: '8px' }}
            />
            <Legend
              wrapperStyle={{ paddingTop: '20px' }}
              iconType="line"
              formatter={(value) => (
                <span style={{ color: '#6b7280', fontSize: '14px', fontWeight: 500 }}>{value}</span>
              )}
            />
            <Line
              type="monotone"
              dataKey="sessions"
              stroke="#3b82f6"
              strokeWidth={2}
              dot={false}
              name="Sessions"
              activeDot={{ r: 6 }}
            />
            <Line
              type="monotone"
              dataKey="users"
              stroke="#10b981"
              strokeWidth={2}
              dot={false}
              name="Users"
              activeDot={{ r: 6 }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Device Breakdown and Forecast */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        {/* Device Breakdown Pie Chart */}
        <div className="bg-gray-50 rounded-lg p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Device Breakdown</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={deviceData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, percentage }) => `${name}: ${percentage.toFixed(1)}%`}
                outerRadius={100}
                fill="#8884d8"
                dataKey="value"
              >
                {deviceData.map((entry, index) => {
                  const device = entry.name.toLowerCase();
                  const color =
                    DEVICE_COLORS[device as keyof typeof DEVICE_COLORS] || '#6b7280';
                  return <Cell key={`cell-${index}`} fill={color} />;
                })}
              </Pie>
              <Tooltip
                formatter={(value: number) => [formatNumber(value), 'Sessions']}
                contentStyle={{
                  backgroundColor: '#fff',
                  border: '1px solid #e5e7eb',
                  borderRadius: '8px',
                  padding: '12px',
                }}
              />
            </PieChart>
          </ResponsiveContainer>
          <div className="mt-4 space-y-2">
            {data.device_breakdown.map((device) => {
              const deviceName = device.device.charAt(0).toUpperCase() + device.device.slice(1);
              const color =
                DEVICE_COLORS[device.device as keyof typeof DEVICE_COLORS] || '#6b7280';
              const Icon =
                device.device === 'desktop'
                  ? Monitor
                  : device.device === 'mobile'
                  ? Smartphone
                  : Tablet;

              return (
                <div key={device.device} className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div
                      className="w-3 h-3 rounded-full"
                      style={{ backgroundColor: color }}
                    ></div>
                    <Icon className="w-4 h-4 text-gray-600" />
                    <span className="text-sm font-medium text-gray-700">{deviceName}</span>
                  </div>
                  <div className="text-sm text-gray-600">
                    {formatNumber(device.sessions)} ({device.percentage.toFixed(1)}%)
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Forecast */}
        <div className="bg-gray-50 rounded-lg p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Forecast</h3>
          <div className="space-y-4">
            {['30d', '60d', '90d'].map((period) => {
              const forecast =
                data.forecast[period as keyof typeof data.forecast];
              const label =
                period === '30d' ? '30 Days' : period === '60d' ? '60 Days' : '90 Days';

              return (
                <div key={period} className="bg-white rounded-lg p-4 border border-gray-200">
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-sm font-semibold text-gray-700">{label}</span>
                    <span className="text-xs text-gray-500">Projected Clicks</span>
                  </div>
                  <div className="text-2xl font-bold text-gray-900 mb-2">
                    {formatNumber(forecast.clicks)}
                  </div>
                  <div className="flex items-center justify-between text-xs text-gray-600">
                    <span>
                      Low: <span className="font-semibold">{formatNumber(forecast.ci_low)}</span>
                    </span>
                    <span>
                      High: <span className="font-semibold">{formatNumber(forecast.ci_high)}</span>
                    </span>
                  </div>
                  <div className="mt-2 h-2 bg-gray-200 rounded-full overflow-hidden">
                    <div
                      className="h-full bg-blue-600 rounded-full"
                      style={{
                        width: `${
                          ((forecast.clicks - forecast.ci_low) /
                            (forecast.ci_high - forecast.ci_low)) *
                          100
                        }%`,
                      }}
                    ></div>
                  </div>
                </div>
              );
            })}
          </div>
          <div className="mt-4 p-4 bg-blue-50 border border-blue-200 rounded-lg">
            <p className="text-sm text-blue-800">
              <strong>Forecast Method:</strong> Based on MSTL decomposition and ARIMA modeling
              of historical trends and seasonality patterns.
            </p>
          </div>
        </div>
      </div>

      {/* Top Pages Table */}
      <div className="bg-gray-50 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Top Landing Pages</h3>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-200">
                <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Page</th>
                <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">
                  Sessions
                </th>
                <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">
                  Users
                </th>
                <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">
                  Bounce Rate
                </th>
                <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">
                  Avg. Duration
                </th>
              </tr>
            </thead>
            <tbody>
              {data.top_pages.slice(0, 10).map((page, index) => (
                <tr
                  key={index}
                  className={`border-b border-gray-100 ${
                    index % 2 === 0 ? 'bg-white' : 'bg-gray-50'
                  }`}
                >
                  <td className="py-3 px-4 text-sm text-gray-900 font-medium max-w-xs truncate">
                    {page.url}
                  </td>
                  <td className="py-3 px-4 text-sm text-gray-700 text-right">
                    {formatNumber(page.sessions)}
                  </td>
                  <td className="py-3 px-4 text-sm text-gray-700 text-right">
                    {formatNumber(page.users)}
                  </td>
                  <td className="py-3 px-4 text-sm text-gray-700 text-right">
                    <span
                      className={`${
                        page.bounce_rate > 70
                          ? 'text-red-600 font-semibold'
                          : page.bounce_rate < 40
                          ? 'text-green-600 font-semibold'
                          : ''
                      }`}
                    >
                      {page.bounce_rate.toFixed(1)}%
                    </span>
                  </td>
                  <td className="py-3 px-4 text-sm text-gray-700 text-right">
                    {Math.floor(page.avg_session_duration / 60)}:
                    {(page.avg_session_duration % 60).toString().padStart(2, '0')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Seasonality and Anomalies */}
      {(data.seasonality || data.anomalies.length > 0) && (
        <div className="mt-8 grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Seasonality */}
          {data.seasonality && (
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-3">Seasonality Patterns</h3>
              <div className="space-y-3">
                <div>
                  <span className="text-sm font-medium text-gray-700">Best Day:</span>
                  <span className="ml-2 text-sm text-gray-900 font-semibold">
                    {data.seasonality.best_day}
                  </span>
                </div>
                <div>
                  <span className="text-sm font-medium text-gray-700">Worst Day:</span>
                  <span className="ml-2 text-sm text-gray-900 font-semibold">
                    {data.seasonality.worst_day}
                  </span>
                </div>
                {data.seasonality.monthly_cycle && (
                  <div>
                    <span className="text-sm font-medium text-gray-700">Monthly Pattern:</span>
                    <p className="mt-1 text-sm text-gray-900">
                      {data.seasonality.cycle_description}
                    </p>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Anomalies */}
          {data.anomalies.length > 0 && (
            <div className="bg-orange-50 border border-orange-200 rounded-lg p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-3">
                Detected Anomalies ({data.anomalies.length})
              </h3>
              <div className="space-y-2 max-h-40 overflow-y-auto">
                {data.anomalies.slice(0, 5).map((anomaly, index) => (
                  <div
                    key={index}
                    className="flex items-center justify-between bg-white rounded p-2"
                  >
                    <div>
                      <span className="text-sm font-medium text-gray-900">
                        {new Date(anomaly.date).toLocaleDateString('en-US', {
                          month: 'short',
                          day: 'numeric',
                          year: 'numeric',
                        })}
                      </span>
                      <span
                        className={`ml-2 text-xs px-2 py-0.5 rounded ${
                          anomaly.type === 'discord'
                            ? 'bg-red-100 text-red-700'
                            : 'bg-blue-100 text-blue-700'
                        }`}
                      >
                        {anomaly.type}
                      </span>
                    </div>
                    <span
                      className={`text-sm font-semibold ${
                        anomaly.magnitude > 0 ? 'text-green-600' : 'text-red-600'
                      }`}
                    >
                      {formatPercent(anomaly.magnitude * 100)}
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
