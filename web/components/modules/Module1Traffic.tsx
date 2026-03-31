import React, { useEffect, useState } from 'react';
import { 
  TrendingUp, 
  TrendingDown, 
  Minus, 
  Users, 
  Eye, 
  MousePointer,
  Activity,
  Smartphone,
  Monitor,
  Tablet,
  AlertCircle,
  Loader2
} from 'lucide-react';
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Area,
  AreaChart
} from 'recharts';

interface MetricCardData {
  label: string;
  value: number;
  change: number;
  changeLabel: string;
  icon: React.ComponentType<{ className?: string }>;
  format?: 'number' | 'decimal' | 'percentage';
}

interface TimeSeriesDataPoint {
  date: string;
  sessions: number;
  users: number;
  pageviews: number;
  bounceRate: number;
  avgSessionDuration: number;
}

interface ChannelData {
  channel: string;
  sessions: number;
  users: number;
  bounceRate: number;
  avgSessionDuration: number;
  conversions: number;
  percentage: number;
}

interface DeviceData {
  device: string;
  sessions: number;
  users: number;
  bounceRate: number;
  avgSessionDuration: number;
  percentage: number;
}

interface Module1Data {
  overview: {
    totalSessions: number;
    totalUsers: number;
    totalPageviews: number;
    avgBounceRate: number;
    avgSessionDuration: number;
    sessionsChange: number;
    usersChange: number;
    pageviewsChange: number;
  };
  timeSeries: TimeSeriesDataPoint[];
  topChannels: ChannelData[];
  deviceBreakdown: DeviceData[];
  dateRange: {
    start: string;
    end: string;
  };
}

interface Module1TrafficProps {
  reportId: string;
}

const Module1Traffic: React.FC<Module1TrafficProps> = ({ reportId }) => {
  const [data, setData] = useState<Module1Data | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);

        const response = await fetch(`/api/reports/${reportId}/modules/1`);
        
        if (!response.ok) {
          throw new Error(`Failed to fetch module data: ${response.statusText}`);
        }

        const moduleData = await response.json();
        setData(moduleData);
      } catch (err) {
        console.error('Error fetching Module 1 data:', err);
        setError(err instanceof Error ? err.message : 'An unknown error occurred');
      } finally {
        setLoading(false);
      }
    };

    if (reportId) {
      fetchData();
    }
  }, [reportId]);

  const formatNumber = (num: number, format?: 'number' | 'decimal' | 'percentage'): string => {
    if (format === 'percentage') {
      return `${num.toFixed(1)}%`;
    }
    if (format === 'decimal') {
      return num.toFixed(2);
    }
    return num.toLocaleString();
  };

  const formatDuration = (seconds: number): string => {
    const minutes = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${minutes}m ${secs}s`;
  };

  const getTrendIcon = (change: number) => {
    if (change > 0) return TrendingUp;
    if (change < 0) return TrendingDown;
    return Minus;
  };

  const getTrendColor = (change: number): string => {
    if (change > 0) return 'text-green-600';
    if (change < 0) return 'text-red-600';
    return 'text-gray-600';
  };

  const MetricCard: React.FC<MetricCardData> = ({ 
    label, 
    value, 
    change, 
    changeLabel, 
    icon: Icon,
    format = 'number'
  }) => {
    const TrendIcon = getTrendIcon(change);
    const trendColor = getTrendColor(change);
    const isPositive = change > 0;
    const isNegative = change < 0;

    return (
      <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200 hover:shadow-lg transition-shadow">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center space-x-3">
            <div className="p-2 bg-blue-50 rounded-lg">
              <Icon className="w-6 h-6 text-blue-600" />
            </div>
            <h3 className="text-sm font-medium text-gray-600">{label}</h3>
          </div>
        </div>
        <div className="space-y-2">
          <p className="text-3xl font-bold text-gray-900">
            {formatNumber(value, format)}
          </p>
          <div className="flex items-center space-x-2">
            <TrendIcon className={`w-4 h-4 ${trendColor}`} />
            <span className={`text-sm font-medium ${trendColor}`}>
              {Math.abs(change).toFixed(1)}%
            </span>
            <span className="text-sm text-gray-500">{changeLabel}</span>
          </div>
        </div>
      </div>
    );
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <div className="text-center">
          <Loader2 className="w-12 h-12 text-blue-600 animate-spin mx-auto mb-4" />
          <p className="text-gray-600 font-medium">Loading traffic data...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6">
        <div className="flex items-start space-x-3">
          <AlertCircle className="w-6 h-6 text-red-600 flex-shrink-0 mt-0.5" />
          <div>
            <h3 className="text-lg font-semibold text-red-900 mb-1">
              Error Loading Traffic Data
            </h3>
            <p className="text-red-700">{error}</p>
            <button 
              onClick={() => window.location.reload()}
              className="mt-4 px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700 transition-colors"
            >
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6">
        <div className="flex items-start space-x-3">
          <AlertCircle className="w-6 h-6 text-yellow-600 flex-shrink-0 mt-0.5" />
          <div>
            <h3 className="text-lg font-semibold text-yellow-900 mb-1">
              No Data Available
            </h3>
            <p className="text-yellow-700">
              Traffic data is not available for this report.
            </p>
          </div>
        </div>
      </div>
    );
  }

  const metricCards: MetricCardData[] = [
    {
      label: 'Total Sessions',
      value: data.overview.totalSessions,
      change: data.overview.sessionsChange,
      changeLabel: 'vs previous period',
      icon: Activity,
      format: 'number'
    },
    {
      label: 'Total Users',
      value: data.overview.totalUsers,
      change: data.overview.usersChange,
      changeLabel: 'vs previous period',
      icon: Users,
      format: 'number'
    },
    {
      label: 'Total Pageviews',
      value: data.overview.totalPageviews,
      change: data.overview.pageviewsChange,
      changeLabel: 'vs previous period',
      icon: Eye,
      format: 'number'
    }
  ];

  const getDeviceIcon = (device: string) => {
    const deviceLower = device.toLowerCase();
    if (deviceLower.includes('mobile')) return Smartphone;
    if (deviceLower.includes('tablet')) return Tablet;
    return Monitor;
  };

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="bg-gradient-to-r from-blue-600 to-blue-700 rounded-lg p-6 text-white">
        <h2 className="text-2xl font-bold mb-2">Traffic Overview</h2>
        <p className="text-blue-100">
          Period: {new Date(data.dateRange.start).toLocaleDateString()} - {new Date(data.dateRange.end).toLocaleDateString()}
        </p>
      </div>

      {/* Metric Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {metricCards.map((card, index) => (
          <MetricCard key={index} {...card} />
        ))}
      </div>

      {/* Traffic Trends Chart */}
      <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
        <h3 className="text-xl font-bold text-gray-900 mb-6">Traffic Trends Over Time</h3>
        <div className="h-[400px]">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={data.timeSeries}>
              <defs>
                <linearGradient id="colorSessions" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3B82F6" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#3B82F6" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="colorUsers" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#10B981" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#10B981" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="colorPageviews" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#8B5CF6" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#8B5CF6" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis 
                dataKey="date" 
                stroke="#6B7280"
                tick={{ fill: '#6B7280', fontSize: 12 }}
                tickFormatter={(value) => new Date(value).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
              />
              <YAxis 
                stroke="#6B7280"
                tick={{ fill: '#6B7280', fontSize: 12 }}
                tickFormatter={(value) => value.toLocaleString()}
              />
              <Tooltip 
                contentStyle={{ 
                  backgroundColor: '#FFFFFF',
                  border: '1px solid #E5E7EB',
                  borderRadius: '8px',
                  boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)'
                }}
                labelFormatter={(value) => new Date(value).toLocaleDateString()}
                formatter={(value: number) => [value.toLocaleString(), '']}
              />
              <Legend 
                wrapperStyle={{ paddingTop: '20px' }}
                iconType="line"
              />
              <Area 
                type="monotone" 
                dataKey="sessions" 
                stroke="#3B82F6" 
                strokeWidth={2}
                fill="url(#colorSessions)"
                name="Sessions"
              />
              <Area 
                type="monotone" 
                dataKey="users" 
                stroke="#10B981" 
                strokeWidth={2}
                fill="url(#colorUsers)"
                name="Users"
              />
              <Area 
                type="monotone" 
                dataKey="pageviews" 
                stroke="#8B5CF6" 
                strokeWidth={2}
                fill="url(#colorPageviews)"
                name="Pageviews"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Two Column Layout: Channels and Devices */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Top Traffic Channels */}
        <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
          <h3 className="text-xl font-bold text-gray-900 mb-6">Top Traffic Channels</h3>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b-2 border-gray-200">
                  <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Channel</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Sessions</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Users</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Bounce Rate</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Share</th>
                </tr>
              </thead>
              <tbody>
                {data.topChannels.map((channel, index) => (
                  <tr 
                    key={index} 
                    className="border-b border-gray-100 hover:bg-gray-50 transition-colors"
                  >
                    <td className="py-4 px-4">
                      <div className="flex items-center space-x-2">
                        <div 
                          className="w-3 h-3 rounded-full"
                          style={{ 
                            backgroundColor: `hsl(${(index * 360) / data.topChannels.length}, 70%, 50%)` 
                          }}
                        />
                        <span className="font-medium text-gray-900">{channel.channel}</span>
                      </div>
                    </td>
                    <td className="py-4 px-4 text-right text-gray-700">
                      {channel.sessions.toLocaleString()}
                    </td>
                    <td className="py-4 px-4 text-right text-gray-700">
                      {channel.users.toLocaleString()}
                    </td>
                    <td className="py-4 px-4 text-right text-gray-700">
                      {channel.bounceRate.toFixed(1)}%
                    </td>
                    <td className="py-4 px-4 text-right">
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                        {channel.percentage.toFixed(1)}%
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          
          {/* Channel Distribution Chart */}
          <div className="mt-6 h-[250px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart 
                data={data.topChannels}
                layout="vertical"
                margin={{ top: 5, right: 30, left: 100, bottom: 5 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                <XAxis 
                  type="number"
                  stroke="#6B7280"
                  tick={{ fill: '#6B7280', fontSize: 12 }}
                  tickFormatter={(value) => value.toLocaleString()}
                />
                <YAxis 
                  type="category"
                  dataKey="channel"
                  stroke="#6B7280"
                  tick={{ fill: '#6B7280', fontSize: 12 }}
                  width={90}
                />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: '#FFFFFF',
                    border: '1px solid #E5E7EB',
                    borderRadius: '8px',
                    boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)'
                  }}
                  formatter={(value: number) => [value.toLocaleString(), 'Sessions']}
                />
                <Bar 
                  dataKey="sessions" 
                  fill="#3B82F6"
                  radius={[0, 4, 4, 0]}
                />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Device Breakdown */}
        <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
          <h3 className="text-xl font-bold text-gray-900 mb-6">Device Breakdown</h3>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b-2 border-gray-200">
                  <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Device</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Sessions</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Users</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Bounce Rate</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Share</th>
                </tr>
              </thead>
              <tbody>
                {data.deviceBreakdown.map((device, index) => {
                  const DeviceIcon = getDeviceIcon(device.device);
                  return (
                    <tr 
                      key={index} 
                      className="border-b border-gray-100 hover:bg-gray-50 transition-colors"
                    >
                      <td className="py-4 px-4">
                        <div className="flex items-center space-x-3">
                          <DeviceIcon className="w-5 h-5 text-gray-600" />
                          <span className="font-medium text-gray-900">{device.device}</span>
                        </div>
                      </td>
                      <td className="py-4 px-4 text-right text-gray-700">
                        {device.sessions.toLocaleString()}
                      </td>
                      <td className="py-4 px-4 text-right text-gray-700">
                        {device.users.toLocaleString()}
                      </td>
                      <td className="py-4 px-4 text-right text-gray-700">
                        {device.bounceRate.toFixed(1)}%
                      </td>
                      <td className="py-4 px-4 text-right">
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800">
                          {device.percentage.toFixed(1)}%
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {/* Device Distribution Visual */}
          <div className="mt-6 space-y-3">
            {data.deviceBreakdown.map((device, index) => {
              const DeviceIcon = getDeviceIcon(device.device);
              return (
                <div key={index} className="space-y-1">
                  <div className="flex items-center justify-between text-sm">
                    <div className="flex items-center space-x-2">
                      <DeviceIcon className="w-4 h-4 text-gray-600" />
                      <span className="font-medium text-gray-700">{device.device}</span>
                    </div>
                    <span className="text-gray-600">{device.percentage.toFixed(1)}%</span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-2.5">
                    <div 
                      className="h-2.5 rounded-full transition-all duration-500"
                      style={{ 
                        width: `${device.percentage}%`,
                        backgroundColor: `hsl(${(index * 120)}, 70%, 50%)`
                      }}
                    />
                  </div>
                </div>
              );
            })}
          </div>

          {/* Summary Stats */}
          <div className="mt-6 pt-6 border-t border-gray-200 grid grid-cols-2 gap-4">
            <div className="text-center p-4 bg-gray-50 rounded-lg">
              <p className="text-sm text-gray-600 mb-1">Avg. Bounce Rate</p>
              <p className="text-2xl font-bold text-gray-900">
                {data.overview.avgBounceRate.toFixed(1)}%
              </p>
            </div>
            <div className="text-center p-4 bg-gray-50 rounded-lg">
              <p className="text-sm text-gray-600 mb-1">Avg. Session Duration</p>
              <p className="text-2xl font-bold text-gray-900">
                {formatDuration(data.overview.avgSessionDuration)}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Engagement Metrics Chart */}
      <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
        <h3 className="text-xl font-bold text-gray-900 mb-6">Engagement Metrics Over Time</h3>
        <div className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data.timeSeries}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis 
                dataKey="date" 
                stroke="#6B7280"
                tick={{ fill: '#6B7280', fontSize: 12 }}
                tickFormatter={(value) => new Date(value).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
              />
              <YAxis 
                yAxisId="left"
                stroke="#6B7280"
                tick={{ fill: '#6B7280', fontSize: 12 }}
                tickFormatter={(value) => `${value}%`}
              />
              <YAxis 
                yAxisId="right"
                orientation="right"
                stroke="#6B7280"
                tick={{ fill: '#6B7280', fontSize: 12 }}
                tickFormatter={(value) => `${value}s`}
              />
              <Tooltip 
                contentStyle={{ 
                  backgroundColor: '#FFFFFF',
                  border: '1px solid #E5E7EB',
                  borderRadius: '8px',
                  boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)'
                }}
                labelFormatter={(value) => new Date(value).toLocaleDateString()}
                formatter={(value: number, name: string) => {
                  if (name === 'Avg. Session Duration') {
                    return [formatDuration(value), name];
                  }
                  return [`${value.toFixed(1)}%`, name];
                }}
              />
              <Legend 
                wrapperStyle={{ paddingTop: '20px' }}
                iconType="line"
              />
              <Line 
                yAxisId="left"
                type="monotone" 
                dataKey="bounceRate" 
                stroke="#EF4444" 
                strokeWidth={2}
                dot={false}
                name="Bounce Rate"
              />
              <Line 
                yAxisId="right"
                type="monotone" 
                dataKey="avgSessionDuration" 
                stroke="#8B5CF6" 
                strokeWidth={2}
                dot={false}
                name="Avg. Session Duration"
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
};

export default Module1Traffic;
