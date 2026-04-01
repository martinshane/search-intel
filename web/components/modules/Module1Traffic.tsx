import React, { useEffect, useState } from 'react';
import { 
  TrendingUp, 
  TrendingDown, 
  Minus, 
  Users, 
  Eye, 
  MousePointer,
  Activity,
  Clock,
  AlertCircle,
  Loader2
} from 'lucide-react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';

interface MetricCardData {
  label: string;
  value: number;
  change: number;
  changeLabel: string;
  icon: React.ComponentType<{ className?: string }>;
  format?: 'number' | 'decimal' | 'percentage' | 'duration';
}

interface TimeSeriesDataPoint {
  date: string;
  sessions: number;
  users: number;
  pageviews: number;
  bounceRate: number;
  avgSessionDuration: number;
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
    bounceRateChange: number;
    sessionDurationChange: number;
  };
  timeSeries: TimeSeriesDataPoint[];
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

        const response = await fetch(`/api/modules/1/${reportId}`);
        
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

  const formatNumber = (num: number, format?: 'number' | 'decimal' | 'percentage' | 'duration'): string => {
    if (format === 'percentage') {
      return `${num.toFixed(1)}%`;
    }
    if (format === 'decimal') {
      return num.toFixed(2);
    }
    if (format === 'duration') {
      return formatDuration(num);
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

  const getTrendColor = (change: number, inverse: boolean = false): string => {
    const isPositive = inverse ? change < 0 : change > 0;
    const isNegative = inverse ? change > 0 : change < 0;
    
    if (isPositive) return 'text-green-600';
    if (isNegative) return 'text-red-600';
    return 'text-gray-600';
  };

  const MetricCard: React.FC<MetricCardData> = ({ label, value, change, changeLabel, icon: Icon, format }) => {
    const TrendIcon = getTrendIcon(change);
    const isInverse = label.toLowerCase().includes('bounce');
    const trendColor = getTrendColor(change, isInverse);

    return (
      <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
        <div className="flex items-center justify-between mb-4">
          <div className="p-2 bg-blue-50 rounded-lg">
            <Icon className="w-6 h-6 text-blue-600" />
          </div>
          <div className={`flex items-center space-x-1 text-sm ${trendColor}`}>
            <TrendIcon className="w-4 h-4" />
            <span className="font-semibold">{Math.abs(change).toFixed(1)}%</span>
          </div>
        </div>
        <div>
          <p className="text-2xl font-bold text-gray-900 mb-1">
            {formatNumber(value, format)}
          </p>
          <p className="text-sm text-gray-600">{label}</p>
          <p className="text-xs text-gray-500 mt-1">{changeLabel}</p>
        </div>
      </div>
    );
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-16">
        <Loader2 className="w-8 h-8 text-blue-600 animate-spin" />
        <span className="ml-3 text-gray-600">Loading traffic data...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6">
        <div className="flex items-start">
          <AlertCircle className="w-6 h-6 text-red-600 mt-0.5" />
          <div className="ml-3">
            <h3 className="text-red-900 font-semibold">Error Loading Module</h3>
            <p className="text-red-700 text-sm mt-1">{error}</p>
          </div>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-6">
        <p className="text-gray-600">No data available for this module.</p>
      </div>
    );
  }

  const metricCards: MetricCardData[] = [
    {
      label: 'Total Sessions',
      value: data.overview.totalSessions,
      change: data.overview.sessionsChange,
      changeLabel: 'vs. previous period',
      icon: Activity,
      format: 'number'
    },
    {
      label: 'Total Users',
      value: data.overview.totalUsers,
      change: data.overview.usersChange,
      changeLabel: 'vs. previous period',
      icon: Users,
      format: 'number'
    },
    {
      label: 'Page Views',
      value: data.overview.totalPageviews,
      change: data.overview.pageviewsChange,
      changeLabel: 'vs. previous period',
      icon: Eye,
      format: 'number'
    },
    {
      label: 'Bounce Rate',
      value: data.overview.avgBounceRate,
      change: data.overview.bounceRateChange,
      changeLabel: 'vs. previous period',
      icon: MousePointer,
      format: 'percentage'
    },
    {
      label: 'Avg. Session Duration',
      value: data.overview.avgSessionDuration,
      change: data.overview.sessionDurationChange,
      changeLabel: 'vs. previous period',
      icon: Clock,
      format: 'duration'
    }
  ];

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white border border-gray-200 rounded-lg shadow-lg p-4">
          <p className="text-sm font-semibold text-gray-900 mb-2">{label}</p>
          {payload.map((entry: any, index: number) => (
            <div key={index} className="flex items-center justify-between space-x-4 text-sm">
              <span className="flex items-center">
                <span
                  className="inline-block w-3 h-3 rounded-full mr-2"
                  style={{ backgroundColor: entry.color }}
                />
                {entry.name}:
              </span>
              <span className="font-semibold">
                {entry.name === 'Bounce Rate' 
                  ? `${entry.value.toFixed(1)}%`
                  : entry.name === 'Avg Session Duration'
                  ? formatDuration(entry.value)
                  : entry.value.toLocaleString()
                }
              </span>
            </div>
          ))}
        </div>
      );
    }
    return null;
  };

  return (
    <div className="space-y-8">
      {/* Module Header */}
      <div>
        <h2 className="text-2xl font-bold text-gray-900 mb-2">
          Traffic Overview & Health
        </h2>
        <p className="text-gray-600">
          90-day traffic trends and key engagement metrics
          {data.dateRange && (
            <span className="text-sm ml-2">
              ({new Date(data.dateRange.start).toLocaleDateString()} - {new Date(data.dateRange.end).toLocaleDateString()})
            </span>
          )}
        </p>
      </div>

      {/* Metric Cards Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-6">
        {metricCards.map((metric, index) => (
          <MetricCard key={index} {...metric} />
        ))}
      </div>

      {/* Sessions & Users Trend */}
      <div className="bg-white rounded-lg shadow border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">
          Sessions & Users Trend
        </h3>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={data.timeSeries}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis 
              dataKey="date" 
              stroke="#6b7280"
              tick={{ fontSize: 12 }}
              tickFormatter={(value) => {
                const date = new Date(value);
                return `${date.getMonth() + 1}/${date.getDate()}`;
              }}
            />
            <YAxis 
              stroke="#6b7280"
              tick={{ fontSize: 12 }}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend 
              wrapperStyle={{ fontSize: '14px' }}
              iconType="circle"
            />
            <Line 
              type="monotone" 
              dataKey="sessions" 
              name="Sessions"
              stroke="#3b82f6" 
              strokeWidth={2}
              dot={false}
            />
            <Line 
              type="monotone" 
              dataKey="users" 
              name="Users"
              stroke="#8b5cf6" 
              strokeWidth={2}
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Page Views Trend */}
      <div className="bg-white rounded-lg shadow border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">
          Page Views Trend
        </h3>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={data.timeSeries}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis 
              dataKey="date" 
              stroke="#6b7280"
              tick={{ fontSize: 12 }}
              tickFormatter={(value) => {
                const date = new Date(value);
                return `${date.getMonth() + 1}/${date.getDate()}`;
              }}
            />
            <YAxis 
              stroke="#6b7280"
              tick={{ fontSize: 12 }}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend 
              wrapperStyle={{ fontSize: '14px' }}
              iconType="circle"
            />
            <Line 
              type="monotone" 
              dataKey="pageviews" 
              name="Page Views"
              stroke="#10b981" 
              strokeWidth={2}
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Engagement Metrics */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Bounce Rate */}
        <div className="bg-white rounded-lg shadow border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">
            Bounce Rate Trend
          </h3>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={data.timeSeries}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis 
                dataKey="date" 
                stroke="#6b7280"
                tick={{ fontSize: 12 }}
                tickFormatter={(value) => {
                  const date = new Date(value);
                  return `${date.getMonth() + 1}/${date.getDate()}`;
                }}
              />
              <YAxis 
                stroke="#6b7280"
                tick={{ fontSize: 12 }}
                tickFormatter={(value) => `${value}%`}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend 
                wrapperStyle={{ fontSize: '14px' }}
                iconType="circle"
              />
              <Line 
                type="monotone" 
                dataKey="bounceRate" 
                name="Bounce Rate"
                stroke="#f59e0b" 
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Session Duration */}
        <div className="bg-white rounded-lg shadow border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">
            Avg. Session Duration
          </h3>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={data.timeSeries}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis 
                dataKey="date" 
                stroke="#6b7280"
                tick={{ fontSize: 12 }}
                tickFormatter={(value) => {
                  const date = new Date(value);
                  return `${date.getMonth() + 1}/${date.getDate()}`;
                }}
              />
              <YAxis 
                stroke="#6b7280"
                tick={{ fontSize: 12 }}
                tickFormatter={(value) => {
                  const minutes = Math.floor(value / 60);
                  return `${minutes}m`;
                }}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend 
                wrapperStyle={{ fontSize: '14px' }}
                iconType="circle"
              />
              <Line 
                type="monotone" 
                dataKey="avgSessionDuration" 
                name="Avg Session Duration"
                stroke="#ec4899" 
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Insights Summary */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-blue-900 mb-3 flex items-center">
          <Activity className="w-5 h-5 mr-2" />
          Key Insights
        </h3>
        <div className="space-y-2 text-sm text-blue-800">
          <p>
            • <strong>Sessions:</strong> {data.overview.sessionsChange > 0 ? 'Increased' : 'Decreased'} by{' '}
            <span className={data.overview.sessionsChange > 0 ? 'text-green-700 font-semibold' : 'text-red-700 font-semibold'}>
              {Math.abs(data.overview.sessionsChange).toFixed(1)}%
            </span> compared to the previous period
          </p>
          <p>
            • <strong>Users:</strong> {data.overview.usersChange > 0 ? 'Increased' : 'Decreased'} by{' '}
            <span className={data.overview.usersChange > 0 ? 'text-green-700 font-semibold' : 'text-red-700 font-semibold'}>
              {Math.abs(data.overview.usersChange).toFixed(1)}%
            </span> compared to the previous period
          </p>
          <p>
            • <strong>Bounce Rate:</strong> Currently at {data.overview.avgBounceRate.toFixed(1)}%
            {data.overview.bounceRateChange < 0 ? (
              <span className="text-green-700 font-semibold"> (improved by {Math.abs(data.overview.bounceRateChange).toFixed(1)}%)</span>
            ) : data.overview.bounceRateChange > 0 ? (
              <span className="text-red-700 font-semibold"> (increased by {data.overview.bounceRateChange.toFixed(1)}%)</span>
            ) : (
              <span> (unchanged)</span>
            )}
          </p>
          <p>
            • <strong>Session Duration:</strong> Average of {formatDuration(data.overview.avgSessionDuration)}
            {data.overview.sessionDurationChange > 0 ? (
              <span className="text-green-700 font-semibold"> (improved by {data.overview.sessionDurationChange.toFixed(1)}%)</span>
            ) : data.overview.sessionDurationChange < 0 ? (
              <span className="text-red-700 font-semibold"> (decreased by {Math.abs(data.overview.sessionDurationChange).toFixed(1)}%)</span>
            ) : (
              <span> (unchanged)</span>
            )}
          </p>
        </div>
      </div>
    </div>
  );
};

export default Module1Traffic;
