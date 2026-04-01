import React from 'react';
import { TrendingUp, TrendingDown, Minus, Activity } from 'lucide-react';
import { LineChart, Line, ResponsiveContainer, Tooltip } from 'recharts';

interface Module1Data {
  overall_direction: 'strong_growth' | 'growth' | 'flat' | 'decline' | 'strong_decline';
  trend_slope_pct_per_month: number;
  change_points: Array<{
    date: string;
    magnitude: number;
    direction: 'rise' | 'drop';
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
  current_metrics: {
    total_clicks: number;
    total_impressions: number;
    average_ctr: number;
    average_position: number;
  };
  sparkline_data: Array<{
    date: string;
    clicks: number;
    impressions: number;
  }>;
}

interface Module1OverviewProps {
  data: Module1Data;
}

const Module1Overview: React.FC<Module1OverviewProps> = ({ data }) => {
  const getDirectionIcon = () => {
    switch (data.overall_direction) {
      case 'strong_growth':
      case 'growth':
        return <TrendingUp className="w-5 h-5 text-green-600" />;
      case 'strong_decline':
      case 'decline':
        return <TrendingDown className="w-5 h-5 text-red-600" />;
      default:
        return <Minus className="w-5 h-5 text-gray-600" />;
    }
  };

  const getDirectionColor = () => {
    switch (data.overall_direction) {
      case 'strong_growth':
      case 'growth':
        return 'text-green-600';
      case 'strong_decline':
      case 'decline':
        return 'text-red-600';
      default:
        return 'text-gray-600';
    }
  };

  const getDirectionLabel = () => {
    return data.overall_direction
      .split('_')
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
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

  const formatPercentage = (num: number): string => {
    return (num * 100).toFixed(2) + '%';
  };

  const formatPosition = (num: number): string => {
    return num.toFixed(1);
  };

  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white border border-gray-200 rounded-lg shadow-lg p-3">
          <p className="text-xs text-gray-600 mb-1">{payload[0].payload.date}</p>
          <p className="text-sm font-semibold text-gray-900">
            Clicks: {formatNumber(payload[0].value)}
          </p>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="space-y-6">
      {/* Header Section */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Health & Trajectory</h2>
          <p className="text-sm text-gray-600 mt-1">
            Last 90 days performance overview and trend analysis
          </p>
        </div>
        <div className="flex items-center space-x-2">
          {getDirectionIcon()}
          <span className={`text-lg font-semibold ${getDirectionColor()}`}>
            {getDirectionLabel()}
          </span>
        </div>
      </div>

      {/* Key Metrics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {/* Total Clicks Card */}
        <div className="bg-white rounded-lg border border-gray-200 p-6 hover:shadow-md transition-shadow">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-medium text-gray-600">Total Clicks</h3>
            <Activity className="w-4 h-4 text-blue-600" />
          </div>
          <div className="flex items-baseline space-x-2">
            <p className="text-3xl font-bold text-gray-900">
              {formatNumber(data.current_metrics.total_clicks)}
            </p>
            <span className={`text-sm font-medium ${getDirectionColor()}`}>
              {data.trend_slope_pct_per_month > 0 ? '+' : ''}
              {data.trend_slope_pct_per_month.toFixed(1)}%/mo
            </span>
          </div>
          <div className="mt-4 h-12">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.sparkline_data}>
                <Line
                  type="monotone"
                  dataKey="clicks"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  dot={false}
                />
                <Tooltip content={<CustomTooltip />} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Total Impressions Card */}
        <div className="bg-white rounded-lg border border-gray-200 p-6 hover:shadow-md transition-shadow">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-medium text-gray-600">Total Impressions</h3>
            <Activity className="w-4 h-4 text-purple-600" />
          </div>
          <div className="flex items-baseline space-x-2">
            <p className="text-3xl font-bold text-gray-900">
              {formatNumber(data.current_metrics.total_impressions)}
            </p>
          </div>
          <div className="mt-4 h-12">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.sparkline_data}>
                <Line
                  type="monotone"
                  dataKey="impressions"
                  stroke="#9333ea"
                  strokeWidth={2}
                  dot={false}
                />
                <Tooltip
                  content={({ active, payload }: any) => {
                    if (active && payload && payload.length) {
                      return (
                        <div className="bg-white border border-gray-200 rounded-lg shadow-lg p-3">
                          <p className="text-xs text-gray-600 mb-1">
                            {payload[0].payload.date}
                          </p>
                          <p className="text-sm font-semibold text-gray-900">
                            Impressions: {formatNumber(payload[0].value)}
                          </p>
                        </div>
                      );
                    }
                    return null;
                  }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Average CTR Card */}
        <div className="bg-white rounded-lg border border-gray-200 p-6 hover:shadow-md transition-shadow">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-medium text-gray-600">Average CTR</h3>
            <Activity className="w-4 h-4 text-green-600" />
          </div>
          <div className="flex items-baseline space-x-2">
            <p className="text-3xl font-bold text-gray-900">
              {formatPercentage(data.current_metrics.average_ctr)}
            </p>
          </div>
          <div className="mt-4">
            <div className="flex items-center justify-between text-xs text-gray-500">
              <span>Click-through rate</span>
              <span className="font-medium">
                {data.current_metrics.average_ctr > 0.05 ? 'Good' : 'Needs improvement'}
              </span>
            </div>
            <div className="mt-2 w-full bg-gray-200 rounded-full h-2">
              <div
                className={`h-2 rounded-full ${
                  data.current_metrics.average_ctr > 0.05 ? 'bg-green-600' : 'bg-yellow-600'
                }`}
                style={{
                  width: `${Math.min(data.current_metrics.average_ctr * 1000, 100)}%`,
                }}
              ></div>
            </div>
          </div>
        </div>

        {/* Average Position Card */}
        <div className="bg-white rounded-lg border border-gray-200 p-6 hover:shadow-md transition-shadow">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-medium text-gray-600">Average Position</h3>
            <Activity className="w-4 h-4 text-orange-600" />
          </div>
          <div className="flex items-baseline space-x-2">
            <p className="text-3xl font-bold text-gray-900">
              {formatPosition(data.current_metrics.average_position)}
            </p>
          </div>
          <div className="mt-4">
            <div className="flex items-center justify-between text-xs text-gray-500">
              <span>SERP position</span>
              <span className="font-medium">
                {data.current_metrics.average_position <= 10
                  ? 'Page 1'
                  : `Page ${Math.ceil(data.current_metrics.average_position / 10)}`}
              </span>
            </div>
            <div className="mt-2 w-full bg-gray-200 rounded-full h-2">
              <div
                className={`h-2 rounded-full ${
                  data.current_metrics.average_position <= 10
                    ? 'bg-green-600'
                    : data.current_metrics.average_position <= 20
                    ? 'bg-yellow-600'
                    : 'bg-red-600'
                }`}
                style={{
                  width: `${Math.max(5, 100 - data.current_metrics.average_position * 2)}%`,
                }}
              ></div>
            </div>
          </div>
        </div>
      </div>

      {/* Forecast Section */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Forecast</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* 30-day Forecast */}
          <div className="border border-gray-200 rounded-lg p-4">
            <p className="text-sm font-medium text-gray-600 mb-2">30 Days</p>
            <p className="text-2xl font-bold text-gray-900 mb-1">
              {formatNumber(data.forecast['30d'].clicks)}
            </p>
            <p className="text-xs text-gray-500">
              {formatNumber(data.forecast['30d'].ci_low)} -{' '}
              {formatNumber(data.forecast['30d'].ci_high)} clicks
            </p>
          </div>

          {/* 60-day Forecast */}
          <div className="border border-gray-200 rounded-lg p-4">
            <p className="text-sm font-medium text-gray-600 mb-2">60 Days</p>
            <p className="text-2xl font-bold text-gray-900 mb-1">
              {formatNumber(data.forecast['60d'].clicks)}
            </p>
            <p className="text-xs text-gray-500">
              {formatNumber(data.forecast['60d'].ci_low)} -{' '}
              {formatNumber(data.forecast['60d'].ci_high)} clicks
            </p>
          </div>

          {/* 90-day Forecast */}
          <div className="border border-gray-200 rounded-lg p-4">
            <p className="text-sm font-medium text-gray-600 mb-2">90 Days</p>
            <p className="text-2xl font-bold text-gray-900 mb-1">
              {formatNumber(data.forecast['90d'].clicks)}
            </p>
            <p className="text-xs text-gray-500">
              {formatNumber(data.forecast['90d'].ci_low)} -{' '}
              {formatNumber(data.forecast['90d'].ci_high)} clicks
            </p>
          </div>
        </div>
      </div>

      {/* Seasonality Insights */}
      {data.seasonality && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-3">Seasonality Insights</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <p className="text-sm text-gray-600">Best performing day</p>
              <p className="text-lg font-semibold text-gray-900">{data.seasonality.best_day}</p>
            </div>
            <div>
              <p className="text-sm text-gray-600">Worst performing day</p>
              <p className="text-lg font-semibold text-gray-900">{data.seasonality.worst_day}</p>
            </div>
          </div>
          {data.seasonality.monthly_cycle && (
            <div className="mt-4 pt-4 border-t border-blue-200">
              <p className="text-sm text-gray-700">{data.seasonality.cycle_description}</p>
            </div>
          )}
        </div>
      )}

      {/* Change Points */}
      {data.change_points && data.change_points.length > 0 && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-3">Detected Change Points</h3>
          <div className="space-y-3">
            {data.change_points.map((changePoint, index) => (
              <div key={index} className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  {changePoint.direction === 'rise' ? (
                    <TrendingUp className="w-5 h-5 text-green-600" />
                  ) : (
                    <TrendingDown className="w-5 h-5 text-red-600" />
                  )}
                  <div>
                    <p className="text-sm font-medium text-gray-900">
                      {new Date(changePoint.date).toLocaleDateString('en-US', {
                        year: 'numeric',
                        month: 'long',
                        day: 'numeric',
                      })}
                    </p>
                    <p className="text-xs text-gray-600">
                      {changePoint.direction === 'rise' ? 'Traffic increase' : 'Traffic drop'}
                    </p>
                  </div>
                </div>
                <span
                  className={`text-sm font-semibold ${
                    changePoint.direction === 'rise' ? 'text-green-600' : 'text-red-600'
                  }`}
                >
                  {changePoint.direction === 'rise' ? '+' : ''}
                  {(changePoint.magnitude * 100).toFixed(1)}%
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Anomalies */}
      {data.anomalies && data.anomalies.length > 0 && (
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-3">Detected Anomalies</h3>
          <div className="space-y-2">
            {data.anomalies.slice(0, 5).map((anomaly, index) => (
              <div
                key={index}
                className="flex items-center justify-between text-sm py-2 border-b border-gray-200 last:border-0"
              >
                <div className="flex items-center space-x-3">
                  <span className="text-xs bg-gray-200 text-gray-700 px-2 py-1 rounded">
                    {anomaly.type}
                  </span>
                  <span className="text-gray-900">
                    {new Date(anomaly.date).toLocaleDateString('en-US', {
                      year: 'numeric',
                      month: 'short',
                      day: 'numeric',
                    })}
                  </span>
                </div>
                <span className="text-gray-600">
                  {(anomaly.magnitude * 100).toFixed(1)}% deviation
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default Module1Overview;