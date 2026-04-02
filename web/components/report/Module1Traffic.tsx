import React from 'react';
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import { TrendingUp, TrendingDown, Minus, Monitor, Smartphone, Tablet } from 'lucide-react';

interface Module1TrafficProps {
  module1: {
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
    daily_data: Array<{
      date: string;
      clicks: number;
      impressions: number;
      ctr: number;
      position: number;
    }>;
    device_breakdown: Array<{
      device: string;
      clicks: number;
      impressions: number;
      percentage: number;
    }>;
    top_landing_pages: Array<{
      url: string;
      clicks: number;
      impressions: number;
      ctr: number;
      position: number;
      change_pct: number;
    }>;
  };
}

const COLORS = {
  desktop: '#3b82f6',
  mobile: '#10b981',
  tablet: '#f59e0b',
  primary: '#3b82f6',
  secondary: '#8b5cf6',
  success: '#10b981',
  warning: '#f59e0b',
  danger: '#ef4444',
  gray: '#6b7280',
};

const formatNumber = (num: number): string => {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M';
  }
  if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K';
  }
  return num.toFixed(0);
};

const formatDate = (dateString: string): string => {
  const date = new Date(dateString);
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
};

const formatPercent = (num: number): string => {
  return (num * 100).toFixed(1) + '%';
};

const getTrendIcon = (direction: string) => {
  const lowerDir = direction.toLowerCase();
  if (lowerDir.includes('growth') || lowerDir.includes('growing')) {
    return <TrendingUp className="w-5 h-5 text-green-500" />;
  }
  if (lowerDir.includes('decline') || lowerDir.includes('declining')) {
    return <TrendingDown className="w-5 h-5 text-red-500" />;
  }
  return <Minus className="w-5 h-5 text-gray-500" />;
};

const getTrendColor = (direction: string): string => {
  const lowerDir = direction.toLowerCase();
  if (lowerDir.includes('growth') || lowerDir.includes('growing')) {
    return 'text-green-600';
  }
  if (lowerDir.includes('decline') || lowerDir.includes('declining')) {
    return 'text-red-600';
  }
  return 'text-gray-600';
};

const getDeviceIcon = (device: string) => {
  const lowerDevice = device.toLowerCase();
  if (lowerDevice === 'desktop') {
    return <Monitor className="w-5 h-5" />;
  }
  if (lowerDevice === 'mobile') {
    return <Smartphone className="w-5 h-5" />;
  }
  if (lowerDevice === 'tablet') {
    return <Tablet className="w-5 h-5" />;
  }
  return <Monitor className="w-5 h-5" />;
};

export default function Module1Traffic({ module1 }: Module1TrafficProps) {
  // Prepare chart data
  const timeSeriesData = module1.daily_data.map((day) => ({
    date: formatDate(day.date),
    fullDate: day.date,
    clicks: day.clicks,
    impressions: day.impressions,
    ctr: day.ctr * 100,
    position: day.position,
  }));

  // Sample every 7 days for cleaner visualization if dataset is large
  const sampledData =
    timeSeriesData.length > 90
      ? timeSeriesData.filter((_, index) => index % 7 === 0)
      : timeSeriesData;

  // Prepare device breakdown data
  const deviceData = module1.device_breakdown.map((device) => ({
    name: device.device.charAt(0).toUpperCase() + device.device.slice(1),
    value: device.clicks,
    percentage: device.percentage,
  }));

  // Calculate summary metrics
  const totalClicks = module1.daily_data.reduce((sum, day) => sum + day.clicks, 0);
  const totalImpressions = module1.daily_data.reduce((sum, day) => sum + day.impressions, 0);
  const avgCTR = totalClicks / totalImpressions;
  const avgPosition =
    module1.daily_data.reduce((sum, day) => sum + day.position, 0) / module1.daily_data.length;

  // Get last 30 days for comparison
  const last30Days = module1.daily_data.slice(-30);
  const prev30Days = module1.daily_data.slice(-60, -30);
  const last30Clicks = last30Days.reduce((sum, day) => sum + day.clicks, 0);
  const prev30Clicks = prev30Days.reduce((sum, day) => sum + day.clicks, 0);
  const clicksChange = ((last30Clicks - prev30Clicks) / prev30Clicks) * 100;

  return (
    <div className="space-y-8">
      {/* Header with Overall Direction */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-start justify-between">
          <div>
            <h2 className="text-2xl font-bold text-gray-900 mb-2">Traffic Overview</h2>
            <p className="text-gray-600">
              12-month trend analysis and performance metrics
            </p>
          </div>
          <div className="flex items-center gap-3">
            {getTrendIcon(module1.overall_direction)}
            <div className="text-right">
              <div className={`text-lg font-semibold ${getTrendColor(module1.overall_direction)}`}>
                {module1.overall_direction.charAt(0).toUpperCase() +
                  module1.overall_direction.slice(1).replace(/_/g, ' ')}
              </div>
              <div className="text-sm text-gray-600">
                {module1.trend_slope_pct_per_month > 0 ? '+' : ''}
                {module1.trend_slope_pct_per_month.toFixed(1)}% per month
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Key Metrics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="text-sm font-medium text-gray-600 mb-1">Total Clicks</div>
          <div className="text-3xl font-bold text-gray-900 mb-2">
            {formatNumber(totalClicks)}
          </div>
          <div className="flex items-center gap-1 text-sm">
            {clicksChange >= 0 ? (
              <TrendingUp className="w-4 h-4 text-green-500" />
            ) : (
              <TrendingDown className="w-4 h-4 text-red-500" />
            )}
            <span className={clicksChange >= 0 ? 'text-green-600' : 'text-red-600'}>
              {clicksChange >= 0 ? '+' : ''}
              {clicksChange.toFixed(1)}%
            </span>
            <span className="text-gray-500">vs prev 30d</span>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="text-sm font-medium text-gray-600 mb-1">Total Impressions</div>
          <div className="text-3xl font-bold text-gray-900 mb-2">
            {formatNumber(totalImpressions)}
          </div>
          <div className="text-sm text-gray-500">
            Last {module1.daily_data.length} days
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="text-sm font-medium text-gray-600 mb-1">Average CTR</div>
          <div className="text-3xl font-bold text-gray-900 mb-2">
            {formatPercent(avgCTR)}
          </div>
          <div className="text-sm text-gray-500">
            Across all queries
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="text-sm font-medium text-gray-600 mb-1">Average Position</div>
          <div className="text-3xl font-bold text-gray-900 mb-2">
            {avgPosition.toFixed(1)}
          </div>
          <div className="text-sm text-gray-500">
            Weighted by impressions
          </div>
        </div>
      </div>

      {/* Main Traffic Chart */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">
          Clicks & Impressions Over Time
        </h3>
        <ResponsiveContainer width="100%" height={400}>
          <AreaChart data={sampledData}>
            <defs>
              <linearGradient id="colorClicks" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={COLORS.primary} stopOpacity={0.3} />
                <stop offset="95%" stopColor={COLORS.primary} stopOpacity={0} />
              </linearGradient>
              <linearGradient id="colorImpressions" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={COLORS.secondary} stopOpacity={0.3} />
                <stop offset="95%" stopColor={COLORS.secondary} stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis
              dataKey="date"
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
            />
            <YAxis
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
              tickFormatter={formatNumber}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: '#fff',
                border: '1px solid #e5e7eb',
                borderRadius: '8px',
                boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)',
              }}
              formatter={(value: number) => formatNumber(value)}
            />
            <Legend />
            <Area
              type="monotone"
              dataKey="impressions"
              stroke={COLORS.secondary}
              strokeWidth={2}
              fillOpacity={1}
              fill="url(#colorImpressions)"
              name="Impressions"
            />
            <Area
              type="monotone"
              dataKey="clicks"
              stroke={COLORS.primary}
              strokeWidth={2}
              fillOpacity={1}
              fill="url(#colorClicks)"
              name="Clicks"
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Change Points & Anomalies */}
      {(module1.change_points.length > 0 || module1.anomalies.length > 0) && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">
            Significant Traffic Events
          </h3>
          <div className="space-y-3">
            {module1.change_points.map((cp, index) => (
              <div
                key={`cp-${index}`}
                className="flex items-start gap-3 p-3 bg-yellow-50 border border-yellow-200 rounded-lg"
              >
                <div className="mt-0.5">
                  {cp.direction === 'drop' ? (
                    <TrendingDown className="w-5 h-5 text-red-500" />
                  ) : (
                    <TrendingUp className="w-5 h-5 text-green-500" />
                  )}
                </div>
                <div className="flex-1">
                  <div className="font-medium text-gray-900">
                    Change Point Detected - {formatDate(cp.date)}
                  </div>
                  <div className="text-sm text-gray-600 mt-1">
                    {cp.direction === 'drop' ? 'Decline' : 'Increase'} of{' '}
                    {Math.abs(cp.magnitude * 100).toFixed(1)}% in traffic
                  </div>
                </div>
              </div>
            ))}
            {module1.anomalies.map((anomaly, index) => (
              <div
                key={`anomaly-${index}`}
                className="flex items-start gap-3 p-3 bg-blue-50 border border-blue-200 rounded-lg"
              >
                <div className="mt-0.5">
                  <div className="w-5 h-5 rounded-full bg-blue-500" />
                </div>
                <div className="flex-1">
                  <div className="font-medium text-gray-900">
                    Anomaly - {formatDate(anomaly.date)}
                  </div>
                  <div className="text-sm text-gray-600 mt-1">
                    {anomaly.type === 'discord' ? 'Unusual pattern' : 'Recurring pattern'} detected
                    (magnitude: {(anomaly.magnitude * 100).toFixed(1)}%)
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Seasonality Insights */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">
          Seasonality Patterns
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="p-4 bg-gray-50 rounded-lg">
            <div className="text-sm font-medium text-gray-600 mb-1">Best Day</div>
            <div className="text-xl font-bold text-green-600">
              {module1.seasonality.best_day}
            </div>
          </div>
          <div className="p-4 bg-gray-50 rounded-lg">
            <div className="text-sm font-medium text-gray-600 mb-1">Worst Day</div>
            <div className="text-xl font-bold text-red-600">
              {module1.seasonality.worst_day}
            </div>
          </div>
        </div>
        {module1.seasonality.monthly_cycle && (
          <div className="mt-4 p-4 bg-blue-50 border border-blue-200 rounded-lg">
            <div className="font-medium text-blue-900 mb-1">Monthly Pattern Detected</div>
            <div className="text-sm text-blue-700">
              {module1.seasonality.cycle_description}
            </div>
          </div>
        )}
      </div>

      {/* Forecast */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">
          Traffic Forecast
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="p-4 border border-gray-200 rounded-lg">
            <div className="text-sm font-medium text-gray-600 mb-2">30 Days</div>
            <div className="text-2xl font-bold text-gray-900 mb-1">
              {formatNumber(module1.forecast['30d'].clicks)}
            </div>
            <div className="text-xs text-gray-500">
              {formatNumber(module1.forecast['30d'].ci_low)} -{' '}
              {formatNumber(module1.forecast['30d'].ci_high)} clicks
            </div>
          </div>
          <div className="p-4 border border-gray-200 rounded-lg">
            <div className="text-sm font-medium text-gray-600 mb-2">60 Days</div>
            <div className="text-2xl font-bold text-gray-900 mb-1">
              {formatNumber(module1.forecast['60d'].clicks)}
            </div>
            <div className="text-xs text-gray-500">
              {formatNumber(module1.forecast['60d'].ci_low)} -{' '}
              {formatNumber(module1.forecast['60d'].ci_high)} clicks
            </div>
          </div>
          <div className="p-4 border border-gray-200 rounded-lg">
            <div className="text-sm font-medium text-gray-600 mb-2">90 Days</div>
            <div className="text-2xl font-bold text-gray-900 mb-1">
              {formatNumber(module1.forecast['90d'].clicks)}
            </div>
            <div className="text-xs text-gray-500">
              {formatNumber(module1.forecast['90d'].ci_low)} -{' '}
              {formatNumber(module1.forecast['90d'].ci_high)} clicks
            </div>
          </div>
        </div>
      </div>

      {/* Device Breakdown */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">
          Traffic by Device
        </h3>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <div className="flex items-center justify-center">
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={deviceData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percentage }) =>
                    `${name}: ${(percentage * 100).toFixed(1)}%`
                  }
                  outerRadius={100}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {deviceData.map((entry, index) => {
                    const colors: { [key: string]: string } = {
                      Desktop: COLORS.desktop,
                      Mobile: COLORS.mobile,
                      Tablet: COLORS.tablet,
                    };
                    return <Cell key={`cell-${index}`} fill={colors[entry.name] || COLORS.gray} />;
                  })}
                </Pie>
                <Tooltip
                  formatter={(value: number) => formatNumber(value) + ' clicks'}
                  contentStyle={{
                    backgroundColor: '#fff',
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px',
                    boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)',
                  }}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div className="space-y-3">
            {module1.device_breakdown.map((device) => (
              <div
                key={device.device}
                className="flex items-center justify-between p-4 bg-gray-50 rounded-lg"
              >
                <div className="flex items-center gap-3">
                  {getDeviceIcon(device.device)}
                  <div>
                    <div className="font-medium text-gray-900">
                      {device.device.charAt(0).toUpperCase() + device.device.slice(1)}
                    </div>
                    <div className="text-sm text-gray-600">
                      {formatNumber(device.impressions)} impressions
                    </div>
                  </div>
                </div>
                <div className="text-right">
                  <div className="text-xl font-bold text-gray-900">
                    {formatNumber(device.clicks)}
                  </div>
                  <div className="text-sm text-gray-600">
                    {(device.percentage * 100).toFixed(1)}%
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Top Landing Pages */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">
          Top Landing Pages
        </h3>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-200">
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-600">
                  Page
                </th>
                <th className="text-right py-3 px-4 text-sm font-medium text-gray-600">
                  Clicks
                </th>
                <th className="text-right py-3 px-4 text-sm font-medium text-gray-600">
                  Impressions
                </th>
                <th className="text-right py-3 px-4 text-sm font-medium text-gray-600">
                  CTR
                </th>
                <th className="text-right py-3 px-4 text-sm font-medium text-gray-600">
                  Position
                </th>
                <th className="text-right py-3 px-4 text-sm font-medium text-gray-600">
                  Change
                </th>
              </tr>
            </thead>
            <tbody>
              {module1.top_landing_pages.map((page, index) => (
                <tr
                  key={index}
                  className="border-b border-gray-100 hover:bg-gray-50 transition-colors"
                >
                  <td className="py-3 px-4">
                    <div className="text-sm font-medium text-gray-900 truncate max-w-md">
                      {page.url}
                    </div>
                  </td>
                  <td className="py-3 px-4 text-right text-sm text-gray-900">
                    {formatNumber(page.clicks)}
                  </td>
                  <td className="py-3 px-4 text-right text-sm text-gray-900">
                    {formatNumber(page.impressions)}
                  </td>
                  <td className="py-3 px-4 text-right text-sm text-gray-900">
                    {formatPercent(page.ctr)}
                  </td>
                  <td className="py-3 px-4 text-right text-sm text-gray-900">
                    {page.position.toFixed(1)}
                  </td>
                  <td className="py-3 px-4 text-right">
                    <div
                      className={`inline-flex items-center gap-1 text-sm font-medium ${
                        page.change_pct >= 0 ? 'text-green-600' : 'text-red-600'
                      }`}
                    >
                      {page.change_pct >= 0 ? (
                        <TrendingUp className="w-4 h-4" />
                      ) : (
                        <TrendingDown className="w-4 h-4" />
                      )}
                      {page.change_pct >= 0 ? '+' : ''}
                      {page.change_pct.toFixed(1)}%
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
