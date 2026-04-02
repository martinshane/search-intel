import React from 'react';
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Area,
  AreaChart,
} from 'recharts';
import { TrendingUp, TrendingDown, Minus, MousePointerClick, Eye, Target, Users, Activity, Monitor, Smartphone, Tablet } from 'lucide-react';

interface Module1Data {
  overview: {
    total_sessions: number;
    total_users: number;
    engagement_rate: number;
    sessions_change_pct: number;
    users_change_pct: number;
    engagement_change_pct: number;
  };
  daily_time_series: Array<{
    date: string;
    sessions: number;
    users: number;
    pageviews: number;
  }>;
  device_breakdown: Array<{
    device: string;
    sessions: number;
    percentage: number;
  }>;
  channel_distribution: Array<{
    channel: string;
    sessions: number;
    percentage: number;
  }>;
  overall_direction: string;
  trend_slope_pct_per_month: number;
}

interface Module1TrafficOverviewProps {
  data: Module1Data;
}

const DEVICE_COLORS: Record<string, string> = {
  desktop: '#3b82f6',
  mobile: '#10b981',
  tablet: '#f59e0b',
};

const CHANNEL_COLORS: Record<string, string> = {
  organic: '#3b82f6',
  direct: '#8b5cf6',
  referral: '#10b981',
  social: '#ec4899',
  email: '#f59e0b',
  paid: '#ef4444',
  other: '#6b7280',
};

const Module1TrafficOverview: React.FC<Module1TrafficOverviewProps> = ({ data }) => {
  const formatNumber = (num: number): string => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toLocaleString();
  };

  const formatPercent = (num: number): string => {
    return (num * 100).toFixed(1) + '%';
  };

  const getTrendIcon = (change: number) => {
    if (change > 0.5) {
      return <TrendingUp className="w-4 h-4 text-green-600" />;
    } else if (change < -0.5) {
      return <TrendingDown className="w-4 h-4 text-red-600" />;
    }
    return <Minus className="w-4 h-4 text-gray-500" />;
  };

  const getTrendColor = (change: number): string => {
    if (change > 0.5) return 'text-green-600';
    if (change < -0.5) return 'text-red-600';
    return 'text-gray-600';
  };

  const getDirectionBadge = (direction: string) => {
    const badges: Record<string, { color: string; label: string }> = {
      strong_growth: { color: 'bg-green-100 text-green-800', label: 'Strong Growth' },
      growth: { color: 'bg-green-50 text-green-700', label: 'Growth' },
      flat: { color: 'bg-gray-100 text-gray-700', label: 'Flat' },
      decline: { color: 'bg-orange-100 text-orange-700', label: 'Decline' },
      strong_decline: { color: 'bg-red-100 text-red-800', label: 'Strong Decline' },
    };

    const badge = badges[direction] || badges.flat;
    return (
      <span className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium ${badge.color}`}>
        {badge.label}
      </span>
    );
  };

  const getDeviceIcon = (device: string) => {
    const deviceLower = device.toLowerCase();
    if (deviceLower === 'desktop') return <Monitor className="w-4 h-4" />;
    if (deviceLower === 'mobile') return <Smartphone className="w-4 h-4" />;
    if (deviceLower === 'tablet') return <Tablet className="w-4 h-4" />;
    return <Monitor className="w-4 h-4" />;
  };

  // Prepare sparkline data (last 30 days)
  const sparklineData = data.daily_time_series.slice(-30);

  // Prepare time series chart data
  const timeSeriesData = data.daily_time_series.map((item) => ({
    date: new Date(item.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
    sessions: item.sessions,
    users: item.users,
    pageviews: item.pageviews,
  }));

  // Prepare device breakdown data
  const deviceData = data.device_breakdown.map((item) => ({
    name: item.device.charAt(0).toUpperCase() + item.device.slice(1),
    value: item.sessions,
    percentage: item.percentage,
  }));

  // Prepare channel distribution data
  const channelData = data.channel_distribution
    .sort((a, b) => b.sessions - a.sessions)
    .map((item) => ({
      name: item.channel.charAt(0).toUpperCase() + item.channel.slice(1),
      sessions: item.sessions,
      percentage: item.percentage,
    }));

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-3 border border-gray-200 rounded-lg shadow-lg">
          <p className="text-sm font-medium text-gray-900 mb-2">{label}</p>
          {payload.map((entry: any, index: number) => (
            <p key={index} className="text-sm text-gray-600">
              <span className="font-medium" style={{ color: entry.color }}>
                {entry.name}:
              </span>{' '}
              {formatNumber(entry.value)}
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  const PieTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0];
      return (
        <div className="bg-white p-3 border border-gray-200 rounded-lg shadow-lg">
          <p className="text-sm font-medium text-gray-900">{data.name}</p>
          <p className="text-sm text-gray-600">
            Sessions: {formatNumber(data.value)}
          </p>
          <p className="text-sm text-gray-600">
            Share: {formatPercent(data.payload.percentage / 100)}
          </p>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="space-y-8">
      {/* Header */}
      <div>
        <h2 className="text-3xl font-bold text-gray-900 mb-2">Traffic Overview</h2>
        <p className="text-gray-600">
          Comprehensive traffic metrics and trends for the reporting period
        </p>
      </div>

      {/* Overall Direction Badge */}
      <div className="flex items-center gap-3">
        <span className="text-sm font-medium text-gray-700">Overall Trend:</span>
        {getDirectionBadge(data.overall_direction)}
        <span className="text-sm text-gray-600">
          ({data.trend_slope_pct_per_month > 0 ? '+' : ''}
          {data.trend_slope_pct_per_month.toFixed(1)}% per month)
        </span>
      </div>

      {/* Hero Metrics with Sparklines */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Total Sessions */}
        <div className="bg-white p-6 rounded-lg border border-gray-200 shadow-sm">
          <div className="flex items-start justify-between mb-4">
            <div>
              <div className="flex items-center gap-2 mb-1">
                <Activity className="w-5 h-5 text-blue-600" />
                <p className="text-sm font-medium text-gray-600">Total Sessions</p>
              </div>
              <p className="text-3xl font-bold text-gray-900">
                {formatNumber(data.overview.total_sessions)}
              </p>
            </div>
            <div className="flex items-center gap-1">
              {getTrendIcon(data.overview.sessions_change_pct)}
              <span className={`text-sm font-medium ${getTrendColor(data.overview.sessions_change_pct)}`}>
                {data.overview.sessions_change_pct > 0 ? '+' : ''}
                {formatPercent(data.overview.sessions_change_pct / 100)}
              </span>
            </div>
          </div>
          <div className="h-16">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={sparklineData}>
                <defs>
                  <linearGradient id="sessionsGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <Area
                  type="monotone"
                  dataKey="sessions"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  fill="url(#sessionsGradient)"
                  isAnimationActive={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Total Users */}
        <div className="bg-white p-6 rounded-lg border border-gray-200 shadow-sm">
          <div className="flex items-start justify-between mb-4">
            <div>
              <div className="flex items-center gap-2 mb-1">
                <Users className="w-5 h-5 text-green-600" />
                <p className="text-sm font-medium text-gray-600">Total Users</p>
              </div>
              <p className="text-3xl font-bold text-gray-900">
                {formatNumber(data.overview.total_users)}
              </p>
            </div>
            <div className="flex items-center gap-1">
              {getTrendIcon(data.overview.users_change_pct)}
              <span className={`text-sm font-medium ${getTrendColor(data.overview.users_change_pct)}`}>
                {data.overview.users_change_pct > 0 ? '+' : ''}
                {formatPercent(data.overview.users_change_pct / 100)}
              </span>
            </div>
          </div>
          <div className="h-16">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={sparklineData}>
                <defs>
                  <linearGradient id="usersGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#10b981" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <Area
                  type="monotone"
                  dataKey="users"
                  stroke="#10b981"
                  strokeWidth={2}
                  fill="url(#usersGradient)"
                  isAnimationActive={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Engagement Rate */}
        <div className="bg-white p-6 rounded-lg border border-gray-200 shadow-sm">
          <div className="flex items-start justify-between mb-4">
            <div>
              <div className="flex items-center gap-2 mb-1">
                <Target className="w-5 h-5 text-purple-600" />
                <p className="text-sm font-medium text-gray-600">Engagement Rate</p>
              </div>
              <p className="text-3xl font-bold text-gray-900">
                {formatPercent(data.overview.engagement_rate)}
              </p>
            </div>
            <div className="flex items-center gap-1">
              {getTrendIcon(data.overview.engagement_change_pct)}
              <span className={`text-sm font-medium ${getTrendColor(data.overview.engagement_change_pct)}`}>
                {data.overview.engagement_change_pct > 0 ? '+' : ''}
                {formatPercent(data.overview.engagement_change_pct / 100)}
              </span>
            </div>
          </div>
          <div className="h-16 flex items-center justify-center">
            <div className="w-full bg-gray-200 rounded-full h-4">
              <div
                className="bg-gradient-to-r from-purple-500 to-purple-600 h-4 rounded-full transition-all duration-500"
                style={{ width: formatPercent(data.overview.engagement_rate) }}
              />
            </div>
          </div>
        </div>
      </div>

      {/* Time Series Chart */}
      <div className="bg-white p-6 rounded-lg border border-gray-200 shadow-sm">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Trends</h3>
        <div className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={timeSeriesData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 12 }}
                stroke="#6b7280"
                interval="preserveStartEnd"
              />
              <YAxis
                tick={{ fontSize: 12 }}
                stroke="#6b7280"
                tickFormatter={formatNumber}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend wrapperStyle={{ fontSize: '14px' }} />
              <Line
                type="monotone"
                dataKey="sessions"
                stroke="#3b82f6"
                strokeWidth={2}
                dot={false}
                name="Sessions"
              />
              <Line
                type="monotone"
                dataKey="users"
                stroke="#10b981"
                strokeWidth={2}
                dot={false}
                name="Users"
              />
              <Line
                type="monotone"
                dataKey="pageviews"
                stroke="#8b5cf6"
                strokeWidth={2}
                dot={false}
                name="Pageviews"
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Device Breakdown and Channel Distribution */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Device Breakdown Pie Chart */}
        <div className="bg-white p-6 rounded-lg border border-gray-200 shadow-sm">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Device Breakdown</h3>
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={deviceData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={(entry) => `${entry.name} (${formatPercent(entry.percentage / 100)})`}
                  outerRadius={100}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {deviceData.map((entry, index) => (
                    <Cell
                      key={`cell-${index}`}
                      fill={DEVICE_COLORS[entry.name.toLowerCase()] || '#6b7280'}
                    />
                  ))}
                </Pie>
                <Tooltip content={<PieTooltip />} />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-4 space-y-2">
            {deviceData.map((device, index) => (
              <div key={index} className="flex items-center justify-between text-sm">
                <div className="flex items-center gap-2">
                  <div
                    className="w-3 h-3 rounded-full"
                    style={{
                      backgroundColor: DEVICE_COLORS[device.name.toLowerCase()] || '#6b7280',
                    }}
                  />
                  <span className="flex items-center gap-1">
                    {getDeviceIcon(device.name)}
                    <span className="font-medium text-gray-900">{device.name}</span>
                  </span>
                </div>
                <span className="text-gray-600">
                  {formatNumber(device.value)} ({formatPercent(device.percentage / 100)})
                </span>
              </div>
            ))}
          </div>
        </div>

        {/* Channel Distribution Bar Chart */}
        <div className="bg-white p-6 rounded-lg border border-gray-200 shadow-sm">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Channel Distribution</h3>
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={channelData} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis type="number" tick={{ fontSize: 12 }} stroke="#6b7280" tickFormatter={formatNumber} />
                <YAxis type="category" dataKey="name" tick={{ fontSize: 12 }} stroke="#6b7280" width={80} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="sessions" name="Sessions">
                  {channelData.map((entry, index) => (
                    <Cell
                      key={`cell-${index}`}
                      fill={CHANNEL_COLORS[entry.name.toLowerCase()] || '#6b7280'}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-4 space-y-2">
            {channelData.map((channel, index) => (
              <div key={index} className="flex items-center justify-between text-sm">
                <div className="flex items-center gap-2">
                  <div
                    className="w-3 h-3 rounded-full"
                    style={{
                      backgroundColor: CHANNEL_COLORS[channel.name.toLowerCase()] || '#6b7280',
                    }}
                  />
                  <span className="font-medium text-gray-900">{channel.name}</span>
                </div>
                <span className="text-gray-600">
                  {formatNumber(channel.sessions)} ({formatPercent(channel.percentage / 100)})
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

export default Module1TrafficOverview;
