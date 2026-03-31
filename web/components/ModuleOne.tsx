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
} from 'recharts';
import { TrendingUp, TrendingDown, Minus, AlertCircle } from 'lucide-react';

interface MetricCardData {
  label: string;
  value: number;
  change: number;
  changeLabel: string;
  trend: 'up' | 'down' | 'flat';
}

interface TimeSeriesDataPoint {
  date: string;
  sessions: number;
  users: number;
  pageviews: number;
}

interface TrafficSourceData {
  name: string;
  value: number;
  percentage: number;
}

interface ModuleOneData {
  metrics: {
    totalSessions: MetricCardData;
    totalUsers: MetricCardData;
    totalPageviews: MetricCardData;
    bounceRate: MetricCardData;
    avgSessionDuration: MetricCardData;
    pagesPerSession: MetricCardData;
  };
  timeSeriesData: TimeSeriesDataPoint[];
  trafficSources: TrafficSourceData[];
}

interface ModuleOneProps {
  data: ModuleOneData | null;
  loading?: boolean;
  error?: string | null;
}

const COLORS = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899'];

const ModuleOne: React.FC<ModuleOneProps> = ({ data, loading = false, error = null }) => {
  if (loading) {
    return (
      <div className="w-full bg-white rounded-lg shadow-md p-8">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-200 rounded w-1/4 mb-6"></div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
            {[1, 2, 3, 4, 5, 6].map((i) => (
              <div key={i} className="h-32 bg-gray-200 rounded"></div>
            ))}
          </div>
          <div className="h-96 bg-gray-200 rounded mb-8"></div>
          <div className="h-80 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="w-full bg-white rounded-lg shadow-md p-8">
        <div className="flex items-center justify-center text-red-600">
          <AlertCircle className="w-6 h-6 mr-2" />
          <span className="text-lg font-medium">Error loading traffic overview: {error}</span>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="w-full bg-white rounded-lg shadow-md p-8">
        <div className="flex items-center justify-center text-gray-500">
          <AlertCircle className="w-6 h-6 mr-2" />
          <span className="text-lg font-medium">No traffic data available</span>
        </div>
      </div>
    );
  }

  const formatNumber = (num: number): string => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toLocaleString();
  };

  const formatDuration = (seconds: number): string => {
    const minutes = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${minutes}m ${secs}s`;
  };

  const formatPercentage = (value: number): string => {
    return `${value.toFixed(1)}%`;
  };

  const getTrendIcon = (trend: 'up' | 'down' | 'flat') => {
    switch (trend) {
      case 'up':
        return <TrendingUp className="w-4 h-4" />;
      case 'down':
        return <TrendingDown className="w-4 h-4" />;
      case 'flat':
        return <Minus className="w-4 h-4" />;
    }
  };

  const getTrendColor = (trend: 'up' | 'down' | 'flat') => {
    switch (trend) {
      case 'up':
        return 'text-green-600';
      case 'down':
        return 'text-red-600';
      case 'flat':
        return 'text-gray-600';
    }
  };

  const MetricCard: React.FC<{ data: MetricCardData; formatValue?: (val: number) => string }> = ({
    data,
    formatValue = formatNumber,
  }) => (
    <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
      <div className="text-sm font-medium text-gray-600 mb-2">{data.label}</div>
      <div className="text-3xl font-bold text-gray-900 mb-3">{formatValue(data.value)}</div>
      <div className={`flex items-center text-sm font-medium ${getTrendColor(data.trend)}`}>
        {getTrendIcon(data.trend)}
        <span className="ml-1">
          {data.change > 0 ? '+' : ''}
          {data.change.toFixed(1)}%
        </span>
        <span className="ml-2 text-gray-600 font-normal">{data.changeLabel}</span>
      </div>
    </div>
  );

  const CustomTooltip: React.FC<any> = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-4 rounded-lg shadow-lg border border-gray-200">
          <p className="text-sm font-medium text-gray-900 mb-2">{label}</p>
          {payload.map((entry: any, index: number) => (
            <p key={index} className="text-sm" style={{ color: entry.color }}>
              <span className="font-medium">{entry.name}:</span> {formatNumber(entry.value)}
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  const CustomPieTooltip: React.FC<any> = ({ active, payload }) => {
    if (active && payload && payload.length) {
      const data = payload[0];
      return (
        <div className="bg-white p-4 rounded-lg shadow-lg border border-gray-200">
          <p className="text-sm font-medium text-gray-900">{data.name}</p>
          <p className="text-sm text-gray-700">
            Sessions: <span className="font-medium">{formatNumber(data.value)}</span>
          </p>
          <p className="text-sm text-gray-700">
            Percentage: <span className="font-medium">{data.payload.percentage.toFixed(1)}%</span>
          </p>
        </div>
      );
    }
    return null;
  };

  const renderCustomLabel = (entry: any) => {
    return `${entry.name} (${entry.percentage.toFixed(1)}%)`;
  };

  return (
    <div className="w-full bg-white rounded-lg shadow-md p-8">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Module 1: Traffic Overview</h2>
        <p className="text-gray-600">
          High-level traffic metrics, trends, and source breakdown from Google Analytics 4
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-12">
        <MetricCard data={data.metrics.totalSessions} />
        <MetricCard data={data.metrics.totalUsers} />
        <MetricCard data={data.metrics.totalPageviews} />
        <MetricCard data={data.metrics.bounceRate} formatValue={formatPercentage} />
        <MetricCard
          data={data.metrics.avgSessionDuration}
          formatValue={(val) => formatDuration(val)}
        />
        <MetricCard data={data.metrics.pagesPerSession} formatValue={(val) => val.toFixed(2)} />
      </div>

      <div className="mb-12">
        <h3 className="text-xl font-bold text-gray-900 mb-6">Traffic Trends Over Time</h3>
        <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
          <ResponsiveContainer width="100%" height={400}>
            <LineChart
              data={data.timeSeriesData}
              margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis
                dataKey="date"
                stroke="#6B7280"
                style={{ fontSize: '12px' }}
                tickFormatter={(value) => {
                  const date = new Date(value);
                  return `${date.getMonth() + 1}/${date.getDate()}`;
                }}
              />
              <YAxis stroke="#6B7280" style={{ fontSize: '12px' }} tickFormatter={formatNumber} />
              <Tooltip content={<CustomTooltip />} />
              <Legend
                wrapperStyle={{ paddingTop: '20px' }}
                iconType="line"
                formatter={(value) => <span className="text-sm font-medium">{value}</span>}
              />
              <Line
                type="monotone"
                dataKey="sessions"
                stroke="#3B82F6"
                strokeWidth={2}
                dot={false}
                name="Sessions"
                activeDot={{ r: 6 }}
              />
              <Line
                type="monotone"
                dataKey="users"
                stroke="#10B981"
                strokeWidth={2}
                dot={false}
                name="Users"
                activeDot={{ r: 6 }}
              />
              <Line
                type="monotone"
                dataKey="pageviews"
                stroke="#F59E0B"
                strokeWidth={2}
                dot={false}
                name="Pageviews"
                activeDot={{ r: 6 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div>
        <h3 className="text-xl font-bold text-gray-900 mb-6">Traffic Sources Breakdown</h3>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
            <ResponsiveContainer width="100%" height={350}>
              <PieChart>
                <Pie
                  data={data.trafficSources}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={renderCustomLabel}
                  outerRadius={120}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {data.trafficSources.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip content={<CustomPieTooltip />} />
              </PieChart>
            </ResponsiveContainer>
          </div>

          <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
            <div className="space-y-4">
              {data.trafficSources.map((source, index) => (
                <div key={index} className="flex items-center justify-between">
                  <div className="flex items-center">
                    <div
                      className="w-4 h-4 rounded mr-3"
                      style={{ backgroundColor: COLORS[index % COLORS.length] }}
                    ></div>
                    <span className="text-sm font-medium text-gray-900">{source.name}</span>
                  </div>
                  <div className="text-right">
                    <div className="text-sm font-bold text-gray-900">
                      {formatNumber(source.value)}
                    </div>
                    <div className="text-xs text-gray-600">{source.percentage.toFixed(1)}%</div>
                  </div>
                </div>
              ))}
            </div>

            <div className="mt-6 pt-6 border-t border-gray-300">
              <div className="flex items-center justify-between">
                <span className="text-sm font-bold text-gray-900">Total Sessions</span>
                <span className="text-sm font-bold text-gray-900">
                  {formatNumber(
                    data.trafficSources.reduce((sum, source) => sum + source.value, 0)
                  )}
                </span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="mt-8 p-4 bg-blue-50 border border-blue-200 rounded-lg">
        <div className="flex items-start">
          <AlertCircle className="w-5 h-5 text-blue-600 mt-0.5 mr-3 flex-shrink-0" />
          <div className="text-sm text-blue-900">
            <p className="font-medium mb-1">Analysis Summary</p>
            <p>
              This module provides a foundational overview of your site's traffic patterns.{' '}
              {data.metrics.totalSessions.trend === 'up'
                ? 'Your traffic is growing, which is a positive signal.'
                : data.metrics.totalSessions.trend === 'down'
                ? 'Your traffic is declining, which warrants investigation in subsequent modules.'
                : 'Your traffic is stable.'}{' '}
              The time series data reveals patterns and trends that will be analyzed in depth in
              Module 1 (Health & Trajectory). Traffic source distribution helps identify dependency
              risks and opportunities for diversification.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ModuleOne;