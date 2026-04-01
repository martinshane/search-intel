import React from 'react';
import {
  LineChart,
  Line,
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
import { TrendingUp, TrendingDown, Minus, MousePointerClick, Eye, Target, Activity } from 'lucide-react';

interface Module1Data {
  overview: {
    total_clicks: number;
    total_impressions: number;
    avg_ctr: number;
    avg_position: number;
    clicks_change_pct: number;
    impressions_change_pct: number;
    ctr_change_pct: number;
    position_change: number;
  };
  daily_time_series: Array<{
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
    ctr: number;
    position: number;
  }>;
  overall_direction: string;
  trend_slope_pct_per_month: number;
}

interface Module1TrafficOverviewProps {
  data: Module1Data;
}

const COLORS = {
  desktop: '#3b82f6',
  mobile: '#10b981',
  tablet: '#f59e0b',
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
    return (num * 100).toFixed(2) + '%';
  };

  const formatPosition = (num: number): string => {
    return num.toFixed(1);
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

  // Prepare chart data
  const timeSeriesData = data.daily_time_series.map((item) => ({
    date: new Date(item.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
    clicks: item.clicks,
    impressions: item.impressions,
    ctr: item.ctr * 100,
    position: item.position,
  }));

  const deviceData = data.device_breakdown.map((item) => ({
    name: item.device.charAt(0).toUpperCase() + item.device.slice(1),
    value: item.clicks,
    clicks: item.clicks,
    impressions: item.impressions,
    ctr: item.ctr,
    position: item.position,
  }));

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-4 rounded-lg shadow-lg border border-gray-200">
          <p className="font-semibold text-gray-900 mb-2">{label}</p>
          {payload.map((entry: any, index: number) => (
            <p key={index} className="text-sm" style={{ color: entry.color }}>
              {entry.name}: {typeof entry.value === 'number' ? formatNumber(entry.value) : entry.value}
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  const CustomPieTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white p-4 rounded-lg shadow-lg border border-gray-200">
          <p className="font-semibold text-gray-900 mb-2">{data.name}</p>
          <p className="text-sm text-gray-700">Clicks: {formatNumber(data.clicks)}</p>
          <p className="text-sm text-gray-700">Impressions: {formatNumber(data.impressions)}</p>
          <p className="text-sm text-gray-700">CTR: {formatPercent(data.ctr)}</p>
          <p className="text-sm text-gray-700">Avg Position: {formatPosition(data.position)}</p>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Traffic Overview</h2>
          <p className="text-gray-600 mt-1">Overall search performance and trends</p>
        </div>
        <div className="flex items-center space-x-3">
          {getDirectionBadge(data.overall_direction)}
          {data.trend_slope_pct_per_month !== undefined && (
            <div className="text-right">
              <p className="text-sm text-gray-600">Monthly Trend</p>
              <p className={`text-lg font-semibold ${getTrendColor(data.trend_slope_pct_per_month)}`}>
                {data.trend_slope_pct_per_month > 0 ? '+' : ''}
                {data.trend_slope_pct_per_month.toFixed(1)}%
              </p>
            </div>
          )}
        </div>
      </div>

      {/* Key Metrics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {/* Total Clicks */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center space-x-2">
              <MousePointerClick className="w-5 h-5 text-blue-600" />
              <h3 className="text-sm font-medium text-gray-600">Total Clicks</h3>
            </div>
            {getTrendIcon(data.overview.clicks_change_pct)}
          </div>
          <p className="text-3xl font-bold text-gray-900">{formatNumber(data.overview.total_clicks)}</p>
          <p className={`text-sm mt-2 ${getTrendColor(data.overview.clicks_change_pct)}`}>
            {data.overview.clicks_change_pct > 0 ? '+' : ''}
            {data.overview.clicks_change_pct.toFixed(1)}% vs previous period
          </p>
        </div>

        {/* Total Impressions */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center space-x-2">
              <Eye className="w-5 h-5 text-purple-600" />
              <h3 className="text-sm font-medium text-gray-600">Total Impressions</h3>
            </div>
            {getTrendIcon(data.overview.impressions_change_pct)}
          </div>
          <p className="text-3xl font-bold text-gray-900">{formatNumber(data.overview.total_impressions)}</p>
          <p className={`text-sm mt-2 ${getTrendColor(data.overview.impressions_change_pct)}`}>
            {data.overview.impressions_change_pct > 0 ? '+' : ''}
            {data.overview.impressions_change_pct.toFixed(1)}% vs previous period
          </p>
        </div>

        {/* Average CTR */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center space-x-2">
              <Target className="w-5 h-5 text-green-600" />
              <h3 className="text-sm font-medium text-gray-600">Average CTR</h3>
            </div>
            {getTrendIcon(data.overview.ctr_change_pct)}
          </div>
          <p className="text-3xl font-bold text-gray-900">{formatPercent(data.overview.avg_ctr)}</p>
          <p className={`text-sm mt-2 ${getTrendColor(data.overview.ctr_change_pct)}`}>
            {data.overview.ctr_change_pct > 0 ? '+' : ''}
            {data.overview.ctr_change_pct.toFixed(1)}% vs previous period
          </p>
        </div>

        {/* Average Position */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center space-x-2">
              <Activity className="w-5 h-5 text-orange-600" />
              <h3 className="text-sm font-medium text-gray-600">Average Position</h3>
            </div>
            {getTrendIcon(-data.overview.position_change)}
          </div>
          <p className="text-3xl font-bold text-gray-900">{formatPosition(data.overview.avg_position)}</p>
          <p className={`text-sm mt-2 ${getTrendColor(-data.overview.position_change)}`}>
            {data.overview.position_change < 0 ? '' : '+'}
            {data.overview.position_change.toFixed(1)} vs previous period
          </p>
        </div>
      </div>

      {/* Charts Row 1: Clicks & Impressions Over Time */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Clicks Over Time */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Clicks Over Time</h3>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={timeSeriesData}>
              <defs>
                <linearGradient id="colorClicks" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="date"
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
                tickFormatter={(value, index) => (index % Math.ceil(timeSeriesData.length / 8) === 0 ? value : '')}
              />
              <YAxis stroke="#6b7280" style={{ fontSize: '12px' }} tickFormatter={formatNumber} />
              <Tooltip content={<CustomTooltip />} />
              <Area
                type="monotone"
                dataKey="clicks"
                stroke="#3b82f6"
                strokeWidth={2}
                fill="url(#colorClicks)"
                name="Clicks"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Impressions Over Time */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Impressions Over Time</h3>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={timeSeriesData}>
              <defs>
                <linearGradient id="colorImpressions" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="date"
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
                tickFormatter={(value, index) => (index % Math.ceil(timeSeriesData.length / 8) === 0 ? value : '')}
              />
              <YAxis stroke="#6b7280" style={{ fontSize: '12px' }} tickFormatter={formatNumber} />
              <Tooltip content={<CustomTooltip />} />
              <Area
                type="monotone"
                dataKey="impressions"
                stroke="#8b5cf6"
                strokeWidth={2}
                fill="url(#colorImpressions)"
                name="Impressions"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Charts Row 2: Position Trend & Device Breakdown */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Average Position Trend */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Average Position Trend</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={timeSeriesData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="date"
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
                tickFormatter={(value, index) => (index % Math.ceil(timeSeriesData.length / 8) === 0 ? value : '')}
              />
              <YAxis
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
                reversed
                domain={['dataMin - 1', 'dataMax + 1']}
                tickFormatter={(value) => value.toFixed(1)}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Line
                type="monotone"
                dataKey="position"
                stroke="#f59e0b"
                strokeWidth={2}
                dot={false}
                name="Position"
              />
            </LineChart>
          </ResponsiveContainer>
          <p className="text-xs text-gray-500 mt-2 text-center">Lower is better (position 1 = top of results)</p>
        </div>

        {/* Device Breakdown */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Clicks by Device</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={deviceData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                outerRadius={100}
                fill="#8884d8"
                dataKey="value"
              >
                {deviceData.map((entry, index) => (
                  <Cell
                    key={`cell-${index}`}
                    fill={COLORS[entry.name.toLowerCase() as keyof typeof COLORS] || '#94a3b8'}
                  />
                ))}
              </Pie>
              <Tooltip content={<CustomPieTooltip />} />
            </PieChart>
          </ResponsiveContainer>
          <div className="mt-4 grid grid-cols-3 gap-4">
            {deviceData.map((device, index) => (
              <div key={index} className="text-center">
                <div
                  className="w-4 h-4 rounded-full mx-auto mb-1"
                  style={{
                    backgroundColor: COLORS[device.name.toLowerCase() as keyof typeof COLORS] || '#94a3b8',
                  }}
                />
                <p className="text-xs font-medium text-gray-700">{device.name}</p>
                <p className="text-sm font-semibold text-gray-900">{formatNumber(device.clicks)}</p>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* CTR Over Time */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Click-Through Rate Over Time</h3>
        <ResponsiveContainer width="100%" height={250}>
          <LineChart data={timeSeriesData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis
              dataKey="date"
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
              tickFormatter={(value, index) => (index % Math.ceil(timeSeriesData.length / 10) === 0 ? value : '')}
            />
            <YAxis
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
              tickFormatter={(value) => `${value.toFixed(1)}%`}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend />
            <Line type="monotone" dataKey="ctr" stroke="#10b981" strokeWidth={2} dot={false} name="CTR (%)" />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default Module1TrafficOverview;
