import React from 'react';
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
} from 'recharts';
import { TrendingUp, TrendingDown, Minus, AlertCircle } from 'lucide-react';

interface MetricCardData {
  label: string;
  value: number;
  change: number;
  changeLabel: string;
  trend: 'up' | 'down' | 'flat';
  format?: 'number' | 'percentage' | 'decimal';
}

interface TimeSeriesDataPoint {
  date: string;
  clicks: number;
  impressions: number;
  ctr: number;
  position: number;
}

interface ModuleOneData {
  metrics: {
    totalClicks: MetricCardData;
    totalImpressions: MetricCardData;
    avgCTR: MetricCardData;
    avgPosition: MetricCardData;
  };
  timeSeriesData: TimeSeriesDataPoint[];
  overallDirection: string;
  trendSlopePctPerMonth: number;
  changePoints: Array<{
    date: string;
    magnitude: number;
    direction: string;
  }>;
  seasonality?: {
    bestDay?: string;
    worstDay?: string;
    monthlyCycle?: boolean;
    cycleDescription?: string;
  };
  forecast?: {
    '30d'?: { clicks: number; ci_low: number; ci_high: number };
    '60d'?: { clicks: number; ci_low: number; ci_high: number };
    '90d'?: { clicks: number; ci_low: number; ci_high: number };
  };
}

interface ModuleOneProps {
  data: ModuleOneData | null;
  loading?: boolean;
  error?: string | null;
}

const ModuleOne: React.FC<ModuleOneProps> = ({ data, loading = false, error = null }) => {
  if (loading) {
    return (
      <div className="w-full bg-white rounded-lg shadow-md p-8">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-200 rounded w-1/3 mb-6"></div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
            {[1, 2, 3, 4].map((i) => (
              <div key={i} className="h-32 bg-gray-200 rounded"></div>
            ))}
          </div>
          <div className="h-96 bg-gray-200 rounded mb-8"></div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="h-80 bg-gray-200 rounded"></div>
            <div className="h-80 bg-gray-200 rounded"></div>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="w-full bg-white rounded-lg shadow-md p-8">
        <div className="flex items-center justify-center text-red-600">
          <AlertCircle className="w-6 h-6 mr-2" />
          <span className="text-lg font-medium">Error loading GSC overview: {error}</span>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="w-full bg-white rounded-lg shadow-md p-8">
        <div className="flex items-center justify-center text-gray-500">
          <AlertCircle className="w-6 h-6 mr-2" />
          <span className="text-lg font-medium">No GSC data available</span>
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

  const formatPercentage = (value: number, decimals: number = 1): string => {
    return `${value.toFixed(decimals)}%`;
  };

  const formatPosition = (value: number): string => {
    return value.toFixed(1);
  };

  const formatMetricValue = (value: number, format?: string): string => {
    switch (format) {
      case 'percentage':
        return formatPercentage(value);
      case 'decimal':
        return value.toFixed(2);
      default:
        return formatNumber(value);
    }
  };

  const getTrendIcon = (trend: 'up' | 'down' | 'flat') => {
    switch (trend) {
      case 'up':
        return <TrendingUp className="w-5 h-5 text-green-600" />;
      case 'down':
        return <TrendingDown className="w-5 h-5 text-red-600" />;
      default:
        return <Minus className="w-5 h-5 text-gray-600" />;
    }
  };

  const getTrendColorClass = (trend: 'up' | 'down' | 'flat') => {
    switch (trend) {
      case 'up':
        return 'text-green-600';
      case 'down':
        return 'text-red-600';
      default:
        return 'text-gray-600';
    }
  };

  const getDirectionBadgeColor = (direction: string) => {
    if (direction.includes('growth')) return 'bg-green-100 text-green-800';
    if (direction.includes('decline')) return 'bg-red-100 text-red-800';
    return 'bg-gray-100 text-gray-800';
  };

  const MetricCard: React.FC<{ metric: MetricCardData }> = ({ metric }) => (
    <div className="bg-white border border-gray-200 rounded-lg p-6 hover:shadow-md transition-shadow">
      <div className="flex items-center justify-between mb-2">
        <span className="text-sm font-medium text-gray-600">{metric.label}</span>
        {getTrendIcon(metric.trend)}
      </div>
      <div className="text-3xl font-bold text-gray-900 mb-2">
        {formatMetricValue(metric.value, metric.format)}
      </div>
      <div className="flex items-center text-sm">
        <span className={`font-medium ${getTrendColorClass(metric.trend)}`}>
          {metric.change > 0 ? '+' : ''}
          {formatPercentage(metric.change)}
        </span>
        <span className="text-gray-500 ml-2">{metric.changeLabel}</span>
      </div>
    </div>
  );

  const formatDate = (dateStr: string): string => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  const formatFullDate = (dateStr: string): string => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
  };

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white border border-gray-200 rounded-lg shadow-lg p-4">
          <p className="font-semibold text-gray-900 mb-2">{formatFullDate(label)}</p>
          {payload.map((entry: any, index: number) => (
            <div key={index} className="flex items-center justify-between space-x-4 text-sm">
              <span className="text-gray-600">{entry.name}:</span>
              <span className="font-semibold" style={{ color: entry.color }}>
                {entry.name === 'CTR'
                  ? formatPercentage(entry.value)
                  : entry.name === 'Position'
                  ? formatPosition(entry.value)
                  : formatNumber(entry.value)}
              </span>
            </div>
          ))}
        </div>
      );
    }
    return null;
  };

  return (
    <div className="w-full bg-white rounded-lg shadow-md p-8">
      <div className="mb-8">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-2xl font-bold text-gray-900">Module 1: GSC Overview & Health</h2>
          <span
            className={`px-3 py-1 rounded-full text-sm font-medium ${getDirectionBadgeColor(
              data.overallDirection
            )}`}
          >
            {data.overallDirection.replace('_', ' ').toUpperCase()}
          </span>
        </div>
        <p className="text-gray-600">
          Performance trends and trajectory analysis over the selected period
        </p>
      </div>

      {/* Metrics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <MetricCard metric={data.metrics.totalClicks} />
        <MetricCard metric={data.metrics.totalImpressions} />
        <MetricCard metric={data.metrics.avgCTR} />
        <MetricCard metric={data.metrics.avgPosition} />
      </div>

      {/* Trend Summary */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 mb-8">
        <h3 className="text-lg font-semibold text-gray-900 mb-3">Trajectory Analysis</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <p className="text-sm text-gray-600 mb-1">Monthly Trend</p>
            <p className="text-xl font-bold text-gray-900">
              {data.trendSlopePctPerMonth > 0 ? '+' : ''}
              {formatPercentage(data.trendSlopePctPerMonth, 2)} per month
            </p>
          </div>
          {data.seasonality && (
            <div>
              <p className="text-sm text-gray-600 mb-1">Seasonality Pattern</p>
              <p className="text-sm font-medium text-gray-900">
                {data.seasonality.bestDay && `Best: ${data.seasonality.bestDay}`}
                {data.seasonality.worstDay && ` • Worst: ${data.seasonality.worstDay}`}
              </p>
              {data.seasonality.cycleDescription && (
                <p className="text-sm text-gray-600 mt-1">{data.seasonality.cycleDescription}</p>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Time Series Chart - Clicks & Impressions */}
      <div className="mb-8">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Clicks & Impressions Trend</h3>
        <div className="bg-gray-50 rounded-lg p-4">
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={data.timeSeriesData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="date"
                tickFormatter={formatDate}
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
              />
              <YAxis
                yAxisId="left"
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
                tickFormatter={formatNumber}
              />
              <YAxis
                yAxisId="right"
                orientation="right"
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
                tickFormatter={formatNumber}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Line
                yAxisId="left"
                type="monotone"
                dataKey="clicks"
                stroke="#3B82F6"
                strokeWidth={2}
                dot={false}
                name="Clicks"
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="impressions"
                stroke="#10B981"
                strokeWidth={2}
                dot={false}
                name="Impressions"
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Time Series Chart - CTR & Position */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
        <div>
          <h3 className="text-lg font-semibold text-gray-900 mb-4">CTR Trend</h3>
          <div className="bg-gray-50 rounded-lg p-4">
            <ResponsiveContainer width="100%" height={250}>
              <LineChart data={data.timeSeriesData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis
                  dataKey="date"
                  tickFormatter={formatDate}
                  stroke="#6b7280"
                  style={{ fontSize: '12px' }}
                />
                <YAxis
                  stroke="#6b7280"
                  style={{ fontSize: '12px' }}
                  tickFormatter={(value) => formatPercentage(value)}
                />
                <Tooltip content={<CustomTooltip />} />
                <Line
                  type="monotone"
                  dataKey="ctr"
                  stroke="#F59E0B"
                  strokeWidth={2}
                  dot={false}
                  name="CTR"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div>
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Average Position</h3>
          <div className="bg-gray-50 rounded-lg p-4">
            <ResponsiveContainer width="100%" height={250}>
              <LineChart data={data.timeSeriesData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis
                  dataKey="date"
                  tickFormatter={formatDate}
                  stroke="#6b7280"
                  style={{ fontSize: '12px' }}
                />
                <YAxis
                  reversed
                  stroke="#6b7280"
                  style={{ fontSize: '12px' }}
                  tickFormatter={(value) => value.toFixed(1)}
                />
                <Tooltip content={<CustomTooltip />} />
                <Line
                  type="monotone"
                  dataKey="position"
                  stroke="#8B5CF6"
                  strokeWidth={2}
                  dot={false}
                  name="Position"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Change Points */}
      {data.changePoints && data.changePoints.length > 0 && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6 mb-8">
          <h3 className="text-lg font-semibold text-gray-900 mb-3">Detected Change Points</h3>
          <div className="space-y-3">
            {data.changePoints.map((changePoint, index) => (
              <div
                key={index}
                className="flex items-center justify-between bg-white rounded p-3 border border-yellow-300"
              >
                <div className="flex items-center space-x-4">
                  {changePoint.direction === 'drop' ? (
                    <TrendingDown className="w-5 h-5 text-red-600" />
                  ) : (
                    <TrendingUp className="w-5 h-5 text-green-600" />
                  )}
                  <div>
                    <p className="font-semibold text-gray-900">
                      {formatFullDate(changePoint.date)}
                    </p>
                    <p className="text-sm text-gray-600">
                      {changePoint.direction === 'drop' ? 'Traffic drop' : 'Traffic increase'}{' '}
                      detected
                    </p>
                  </div>
                </div>
                <div className="text-right">
                  <p
                    className={`text-lg font-bold ${
                      changePoint.magnitude < 0 ? 'text-red-600' : 'text-green-600'
                    }`}
                  >
                    {changePoint.magnitude > 0 ? '+' : ''}
                    {formatPercentage(changePoint.magnitude * 100)}
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Forecast */}
      {data.forecast && (
        <div className="bg-purple-50 border border-purple-200 rounded-lg p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Forecast</h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {data.forecast['30d'] && (
              <div className="bg-white rounded-lg p-4 border border-purple-300">
                <p className="text-sm text-gray-600 mb-1">30 Days</p>
                <p className="text-2xl font-bold text-gray-900">
                  {formatNumber(data.forecast['30d'].clicks)} clicks
                </p>
                <p className="text-xs text-gray-500 mt-1">
                  Range: {formatNumber(data.forecast['30d'].ci_low)} -{' '}
                  {formatNumber(data.forecast['30d'].ci_high)}
                </p>
              </div>
            )}
            {data.forecast['60d'] && (
              <div className="bg-white rounded-lg p-4 border border-purple-300">
                <p className="text-sm text-gray-600 mb-1">60 Days</p>
                <p className="text-2xl font-bold text-gray-900">
                  {formatNumber(data.forecast['60d'].clicks)} clicks
                </p>
                <p className="text-xs text-gray-500 mt-1">
                  Range: {formatNumber(data.forecast['60d'].ci_low)} -{' '}
                  {formatNumber(data.forecast['60d'].ci_high)}
                </p>
              </div>
            )}
            {data.forecast['90d'] && (
              <div className="bg-white rounded-lg p-4 border border-purple-300">
                <p className="text-sm text-gray-600 mb-1">90 Days</p>
                <p className="text-2xl font-bold text-gray-900">
                  {formatNumber(data.forecast['90d'].clicks)} clicks
                </p>
                <p className="text-xs text-gray-500 mt-1">
                  Range: {formatNumber(data.forecast['90d'].ci_low)} -{' '}
                  {formatNumber(data.forecast['90d'].ci_high)}
                </p>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default ModuleOne;