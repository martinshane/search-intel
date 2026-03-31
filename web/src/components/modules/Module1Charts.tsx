import React from 'react';
import {
  LineChart,
  Line,
  PieChart,
  Pie,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,
} from 'recharts';
import { TrendingUp, TrendingDown, Minus } from 'lucide-react';

interface Module1Data {
  overall_direction: string;
  trend_slope_pct_per_month: number;
  change_points?: Array<{
    date: string;
    magnitude: number;
    direction: string;
  }>;
  seasonality?: {
    best_day: string;
    worst_day: string;
    monthly_cycle: boolean;
    cycle_description?: string;
  };
  anomalies?: Array<{
    date: string;
    type: string;
    magnitude: number;
  }>;
  forecast?: {
    '30d': { clicks: number; ci_low: number; ci_high: number };
    '60d': { clicks: number; ci_low: number; ci_high: number };
    '90d': { clicks: number; ci_low: number; ci_high: number };
  };
  daily_data?: Array<{
    date: string;
    clicks: number;
    impressions: number;
    ctr: number;
    position: number;
  }>;
  device_breakdown?: Array<{
    device: string;
    sessions: number;
    percentage: number;
  }>;
  country_data?: Array<{
    country: string;
    sessions: number;
    percentage: number;
  }>;
}

interface Module1ChartsProps {
  data: Module1Data | null;
  loading?: boolean;
  error?: string | null;
}

const COLORS = {
  primary: '#3b82f6',
  secondary: '#8b5cf6',
  success: '#10b981',
  warning: '#f59e0b',
  danger: '#ef4444',
  neutral: '#6b7280',
};

const DEVICE_COLORS: Record<string, string> = {
  desktop: '#3b82f6',
  mobile: '#8b5cf6',
  tablet: '#10b981',
  DESKTOP: '#3b82f6',
  MOBILE: '#8b5cf6',
  TABLET: '#10b981',
};

const getTrendIcon = (direction: string) => {
  const normalized = direction.toLowerCase();
  if (normalized.includes('growth') || normalized.includes('growing')) {
    return <TrendingUp className="w-5 h-5 text-green-500" />;
  } else if (normalized.includes('decline') || normalized.includes('declining')) {
    return <TrendingDown className="w-5 h-5 text-red-500" />;
  }
  return <Minus className="w-5 h-5 text-gray-500" />;
};

const getTrendColor = (direction: string) => {
  const normalized = direction.toLowerCase();
  if (normalized.includes('growth') || normalized.includes('growing')) {
    return 'text-green-600';
  } else if (normalized.includes('decline') || normalized.includes('declining')) {
    return 'text-red-600';
  }
  return 'text-gray-600';
};

const formatNumber = (num: number): string => {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M';
  } else if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K';
  }
  return num.toFixed(0);
};

const formatDate = (dateString: string): string => {
  const date = new Date(dateString);
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
};

const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-white p-3 border border-gray-200 rounded-lg shadow-lg">
        <p className="text-sm font-medium text-gray-900 mb-2">{label}</p>
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

export const Module1Charts: React.FC<Module1ChartsProps> = ({ data, loading, error }) => {
  if (loading) {
    return (
      <div className="space-y-6">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="animate-pulse">
            <div className="h-6 bg-gray-200 rounded w-1/4 mb-4"></div>
            <div className="h-64 bg-gray-100 rounded"></div>
          </div>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <div className="animate-pulse">
              <div className="h-6 bg-gray-200 rounded w-1/3 mb-4"></div>
              <div className="h-64 bg-gray-100 rounded"></div>
            </div>
          </div>
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <div className="animate-pulse">
              <div className="h-6 bg-gray-200 rounded w-1/3 mb-4"></div>
              <div className="h-64 bg-gray-100 rounded"></div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-red-800 mb-2">Error Loading Traffic Overview</h3>
        <p className="text-red-600">{error}</p>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-6">
        <p className="text-gray-600">No traffic data available</p>
      </div>
    );
  }

  // Prepare daily traffic data for line chart
  const trafficData = data.daily_data?.map((item) => ({
    date: formatDate(item.date),
    fullDate: item.date,
    clicks: item.clicks,
    impressions: item.impressions,
    ctr: (item.ctr * 100).toFixed(2),
    position: item.position.toFixed(1),
  })) || [];

  // Sample data if too many points (show every nth point for performance)
  const sampledTrafficData = trafficData.length > 90 
    ? trafficData.filter((_, index) => index % Math.ceil(trafficData.length / 90) === 0)
    : trafficData;

  // Prepare device breakdown data
  const deviceData = data.device_breakdown?.map((item) => ({
    name: item.device.charAt(0).toUpperCase() + item.device.slice(1).toLowerCase(),
    value: item.sessions,
    percentage: item.percentage,
  })) || [];

  // Prepare country data (top 10)
  const countryData = data.country_data?.slice(0, 10).map((item) => ({
    country: item.country,
    sessions: item.sessions,
    percentage: item.percentage,
  })) || [];

  return (
    <div className="space-y-6">
      {/* Overview Stats */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-medium text-gray-600">Overall Direction</h3>
            {getTrendIcon(data.overall_direction)}
          </div>
          <p className={`text-2xl font-bold ${getTrendColor(data.overall_direction)}`}>
            {data.overall_direction.replace(/_/g, ' ').toUpperCase()}
          </p>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-sm font-medium text-gray-600 mb-2">Monthly Trend</h3>
          <p className={`text-2xl font-bold ${data.trend_slope_pct_per_month >= 0 ? 'text-green-600' : 'text-red-600'}`}>
            {data.trend_slope_pct_per_month >= 0 ? '+' : ''}
            {data.trend_slope_pct_per_month.toFixed(1)}%
          </p>
          <p className="text-xs text-gray-500 mt-1">per month</p>
        </div>

        {data.forecast && (
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-sm font-medium text-gray-600 mb-2">30-Day Forecast</h3>
            <p className="text-2xl font-bold text-gray-900">
              {formatNumber(data.forecast['30d'].clicks)}
            </p>
            <p className="text-xs text-gray-500 mt-1">
              ±{formatNumber(data.forecast['30d'].ci_high - data.forecast['30d'].clicks)} clicks
            </p>
          </div>
        )}
      </div>

      {/* Traffic Trend Chart */}
      {sampledTrafficData.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Trend Over Time</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={sampledTrafficData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis 
                dataKey="date" 
                stroke="#6b7280"
                tick={{ fontSize: 12 }}
              />
              <YAxis 
                stroke="#6b7280"
                tick={{ fontSize: 12 }}
                tickFormatter={formatNumber}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Line 
                type="monotone" 
                dataKey="clicks" 
                stroke={COLORS.primary} 
                strokeWidth={2}
                dot={false}
                name="Clicks"
              />
              <Line 
                type="monotone" 
                dataKey="impressions" 
                stroke={COLORS.secondary} 
                strokeWidth={2}
                dot={false}
                name="Impressions"
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Device Breakdown and Top Countries */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Device Breakdown Pie Chart */}
        {deviceData.length > 0 && (
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Device Breakdown</h3>
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={deviceData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percentage }) => `${name}: ${percentage.toFixed(1)}%`}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {deviceData.map((entry, index) => (
                    <Cell 
                      key={`cell-${index}`} 
                      fill={DEVICE_COLORS[entry.name.toUpperCase()] || COLORS.neutral} 
                    />
                  ))}
                </Pie>
                <Tooltip content={<CustomTooltip />} />
              </PieChart>
            </ResponsiveContainer>
            <div className="mt-4 space-y-2">
              {deviceData.map((device, index) => (
                <div key={index} className="flex items-center justify-between text-sm">
                  <div className="flex items-center">
                    <div 
                      className="w-3 h-3 rounded-full mr-2"
                      style={{ backgroundColor: DEVICE_COLORS[device.name.toUpperCase()] || COLORS.neutral }}
                    ></div>
                    <span className="text-gray-700">{device.name}</span>
                  </div>
                  <span className="font-medium text-gray-900">
                    {formatNumber(device.value)} ({device.percentage.toFixed(1)}%)
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Top Countries Bar Chart */}
        {countryData.length > 0 && (
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Top 10 Countries by Sessions</h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={countryData} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis 
                  type="number" 
                  stroke="#6b7280"
                  tick={{ fontSize: 12 }}
                  tickFormatter={formatNumber}
                />
                <YAxis 
                  type="category" 
                  dataKey="country" 
                  stroke="#6b7280"
                  tick={{ fontSize: 12 }}
                  width={80}
                />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="sessions" fill={COLORS.primary} name="Sessions" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>

      {/* Seasonality and Anomalies */}
      {(data.seasonality || (data.anomalies && data.anomalies.length > 0)) && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Seasonality */}
          {data.seasonality && (
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Seasonality Patterns</h3>
              <div className="space-y-3">
                <div className="flex justify-between items-center py-2 border-b border-gray-100">
                  <span className="text-sm text-gray-600">Best Day</span>
                  <span className="text-sm font-medium text-green-600">{data.seasonality.best_day}</span>
                </div>
                <div className="flex justify-between items-center py-2 border-b border-gray-100">
                  <span className="text-sm text-gray-600">Worst Day</span>
                  <span className="text-sm font-medium text-red-600">{data.seasonality.worst_day}</span>
                </div>
                <div className="flex justify-between items-center py-2 border-b border-gray-100">
                  <span className="text-sm text-gray-600">Monthly Cycle</span>
                  <span className="text-sm font-medium text-gray-900">
                    {data.seasonality.monthly_cycle ? 'Yes' : 'No'}
                  </span>
                </div>
                {data.seasonality.cycle_description && (
                  <div className="mt-3 p-3 bg-blue-50 rounded-lg">
                    <p className="text-sm text-blue-800">{data.seasonality.cycle_description}</p>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Anomalies */}
          {data.anomalies && data.anomalies.length > 0 && (
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Recent Anomalies</h3>
              <div className="space-y-3">
                {data.anomalies.slice(0, 5).map((anomaly, index) => (
                  <div key={index} className="p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
                    <div className="flex justify-between items-start">
                      <div>
                        <p className="text-sm font-medium text-gray-900">
                          {formatDate(anomaly.date)}
                        </p>
                        <p className="text-xs text-gray-600 mt-1">
                          Type: {anomaly.type}
                        </p>
                      </div>
                      <span className={`text-sm font-medium ${anomaly.magnitude < 0 ? 'text-red-600' : 'text-green-600'}`}>
                        {anomaly.magnitude >= 0 ? '+' : ''}
                        {(anomaly.magnitude * 100).toFixed(1)}%
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Forecast */}
      {data.forecast && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Forecast</h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="p-4 bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg">
              <p className="text-sm text-blue-800 font-medium mb-1">30 Days</p>
              <p className="text-2xl font-bold text-blue-900">
                {formatNumber(data.forecast['30d'].clicks)}
              </p>
              <p className="text-xs text-blue-700 mt-1">
                Range: {formatNumber(data.forecast['30d'].ci_low)} - {formatNumber(data.forecast['30d'].ci_high)}
              </p>
            </div>
            <div className="p-4 bg-gradient-to-br from-purple-50 to-purple-100 rounded-lg">
              <p className="text-sm text-purple-800 font-medium mb-1">60 Days</p>
              <p className="text-2xl font-bold text-purple-900">
                {formatNumber(data.forecast['60d'].clicks)}
              </p>
              <p className="text-xs text-purple-700 mt-1">
                Range: {formatNumber(data.forecast['60d'].ci_low)} - {formatNumber(data.forecast['60d'].ci_high)}
              </p>
            </div>
            <div className="p-4 bg-gradient-to-br from-green-50 to-green-100 rounded-lg">
              <p className="text-sm text-green-800 font-medium mb-1">90 Days</p>
              <p className="text-2xl font-bold text-green-900">
                {formatNumber(data.forecast['90d'].clicks)}
              </p>
              <p className="text-xs text-green-700 mt-1">
                Range: {formatNumber(data.forecast['90d'].ci_low)} - {formatNumber(data.forecast['90d'].ci_high)}
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Change Points */}
      {data.change_points && data.change_points.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Significant Changes Detected</h3>
          <div className="space-y-3">
            {data.change_points.map((cp, index) => (
              <div 
                key={index} 
                className={`p-4 rounded-lg border-l-4 ${
                  cp.direction === 'drop' 
                    ? 'bg-red-50 border-red-500' 
                    : 'bg-green-50 border-green-500'
                }`}
              >
                <div className="flex justify-between items-start">
                  <div>
                    <p className="text-sm font-medium text-gray-900">
                      {formatDate(cp.date)}
                    </p>
                    <p className="text-xs text-gray-600 mt-1">
                      {cp.direction === 'drop' ? 'Traffic Drop' : 'Traffic Increase'}
                    </p>
                  </div>
                  <span className={`text-sm font-bold ${
                    cp.direction === 'drop' ? 'text-red-600' : 'text-green-600'
                  }`}>
                    {cp.magnitude >= 0 ? '+' : ''}
                    {(cp.magnitude * 100).toFixed(1)}%
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default Module1Charts;