import React, { useEffect, useState } from 'react';
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
  ComposedChart,
} from 'recharts';
import { TrendingUp, TrendingDown, Minus, AlertCircle, Calendar, Activity } from 'lucide-react';

interface TimeSeriesDataPoint {
  date: string;
  clicks: number;
  impressions: number;
  ctr: number;
  position: number;
}

interface Forecast {
  clicks: number;
  ci_low: number;
  ci_high: number;
}

interface ChangePoint {
  date: string;
  magnitude: number;
  direction: 'drop' | 'spike';
}

interface Seasonality {
  best_day: string;
  worst_day: string;
  monthly_cycle: boolean;
  cycle_description: string;
}

interface Anomaly {
  date: string;
  type: 'discord' | 'motif';
  magnitude: number;
}

interface Summary {
  total_clicks: number;
  total_impressions: number;
  avg_ctr: number;
  avg_position: number;
  clicks_change_pct: number;
  impressions_change_pct: number;
  ctr_change_pct: number;
  position_change: number;
}

interface TrafficOverview {
  timeseries: TimeSeriesDataPoint[];
  summary: Summary;
}

interface TopPage {
  url: string;
  clicks: number;
  impressions: number;
  ctr: number;
  position: number;
  clicks_change_pct: number;
}

interface DeviceBreakdown {
  device: string;
  clicks: number;
  impressions: number;
  percentage: number;
}

interface Module1Data {
  overall_direction: 'strong_growth' | 'growth' | 'flat' | 'decline' | 'strong_decline';
  trend_slope_pct_per_month: number;
  change_points: ChangePoint[];
  seasonality: Seasonality;
  anomalies: Anomaly[];
  forecast: {
    '30d': Forecast;
    '60d': Forecast;
    '90d': Forecast;
  };
  traffic_overview: TrafficOverview;
  top_pages?: TopPage[];
  device_breakdown?: DeviceBreakdown[];
}

interface Module1TrafficOverviewProps {
  reportId: string;
}

const Module1TrafficOverview: React.FC<Module1TrafficOverviewProps> = ({ reportId }) => {
  const [module1, setModule1] = useState<Module1Data | null>(null);
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
        
        const data = await response.json();
        setModule1(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An error occurred while loading data');
      } finally {
        setLoading(false);
      }
    };

    if (reportId) {
      fetchData();
    }
  }, [reportId]);

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
    return num.toFixed(1) + '%';
  };

  const formatPosition = (num: number): string => {
    return num.toFixed(1);
  };

  const getTrendIcon = (direction: string) => {
    switch (direction) {
      case 'strong_growth':
      case 'growth':
        return <TrendingUp className="w-5 h-5 text-green-500" />;
      case 'strong_decline':
      case 'decline':
        return <TrendingDown className="w-5 h-5 text-red-500" />;
      case 'flat':
      default:
        return <Minus className="w-5 h-5 text-gray-500" />;
    }
  };

  const getTrendColor = (direction: string): string => {
    switch (direction) {
      case 'strong_growth':
      case 'growth':
        return 'text-green-600';
      case 'strong_decline':
      case 'decline':
        return 'text-red-600';
      case 'flat':
      default:
        return 'text-gray-600';
    }
  };

  const getTrendLabel = (direction: string): string => {
    switch (direction) {
      case 'strong_growth':
        return 'Strong Growth';
      case 'growth':
        return 'Growth';
      case 'flat':
        return 'Stable';
      case 'decline':
        return 'Decline';
      case 'strong_decline':
        return 'Strong Decline';
      default:
        return direction;
    }
  };

  const getChangeIndicator = (value: number) => {
    if (value > 0) {
      return (
        <span className="flex items-center text-green-600">
          <TrendingUp className="w-4 h-4 mr-1" />
          +{formatPercentage(value)}
        </span>
      );
    } else if (value < 0) {
      return (
        <span className="flex items-center text-red-600">
          <TrendingDown className="w-4 h-4 mr-1" />
          {formatPercentage(value)}
        </span>
      );
    } else {
      return (
        <span className="flex items-center text-gray-600">
          <Minus className="w-4 h-4 mr-1" />
          {formatPercentage(value)}
        </span>
      );
    }
  };

  const COLORS = {
    primary: '#3b82f6',
    secondary: '#8b5cf6',
    success: '#10b981',
    warning: '#f59e0b',
    danger: '#ef4444',
    info: '#06b6d4',
  };

  const DEVICE_COLORS = ['#3b82f6', '#8b5cf6', '#10b981'];

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6">
        <div className="flex items-center">
          <AlertCircle className="w-6 h-6 text-red-600 mr-3" />
          <div>
            <h3 className="text-lg font-semibold text-red-900">Error Loading Data</h3>
            <p className="text-red-700 mt-1">{error}</p>
          </div>
        </div>
      </div>
    );
  }

  if (!module1) {
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-6">
        <p className="text-gray-600">No data available</p>
      </div>
    );
  }

  const { traffic_overview, summary, overall_direction, trend_slope_pct_per_month, change_points, seasonality, anomalies, forecast, top_pages, device_breakdown } = module1;

  return (
    <div className="space-y-8">
      {/* Header Section */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-2xl font-bold text-gray-900">Traffic Health & Trajectory</h2>
          <div className="flex items-center space-x-2">
            {getTrendIcon(overall_direction)}
            <span className={`text-lg font-semibold ${getTrendColor(overall_direction)}`}>
              {getTrendLabel(overall_direction)}
            </span>
          </div>
        </div>
        <p className="text-gray-600">
          Your site is {overall_direction.replace('_', ' ')} at{' '}
          <span className={`font-semibold ${trend_slope_pct_per_month >= 0 ? 'text-green-600' : 'text-red-600'}`}>
            {trend_slope_pct_per_month > 0 ? '+' : ''}{formatPercentage(trend_slope_pct_per_month)}
          </span>{' '}
          per month
        </p>
      </div>

      {/* Key Metrics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-medium text-gray-600">Total Clicks</h3>
            <Activity className="w-5 h-5 text-blue-500" />
          </div>
          <p className="text-3xl font-bold text-gray-900">{formatNumber(summary.total_clicks)}</p>
          <div className="mt-2">
            {getChangeIndicator(summary.clicks_change_pct)}
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-medium text-gray-600">Total Impressions</h3>
            <Activity className="w-5 h-5 text-purple-500" />
          </div>
          <p className="text-3xl font-bold text-gray-900">{formatNumber(summary.total_impressions)}</p>
          <div className="mt-2">
            {getChangeIndicator(summary.impressions_change_pct)}
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-medium text-gray-600">Average CTR</h3>
            <Activity className="w-5 h-5 text-green-500" />
          </div>
          <p className="text-3xl font-bold text-gray-900">{formatPercentage(summary.avg_ctr)}</p>
          <div className="mt-2">
            {getChangeIndicator(summary.ctr_change_pct)}
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-medium text-gray-600">Average Position</h3>
            <Activity className="w-5 h-5 text-orange-500" />
          </div>
          <p className="text-3xl font-bold text-gray-900">{formatPosition(summary.avg_position)}</p>
          <div className="mt-2">
            {summary.position_change < 0 ? (
              <span className="flex items-center text-green-600">
                <TrendingUp className="w-4 h-4 mr-1" />
                {Math.abs(summary.position_change).toFixed(1)} (improved)
              </span>
            ) : summary.position_change > 0 ? (
              <span className="flex items-center text-red-600">
                <TrendingDown className="w-4 h-4 mr-1" />
                +{summary.position_change.toFixed(1)} (declined)
              </span>
            ) : (
              <span className="flex items-center text-gray-600">
                <Minus className="w-4 h-4 mr-1" />
                No change
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Traffic Over Time Chart */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Clicks Over Time</h3>
        <ResponsiveContainer width="100%" height={400}>
          <ComposedChart data={traffic_overview.timeseries}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis 
              dataKey="date" 
              tick={{ fill: '#6b7280', fontSize: 12 }}
              tickFormatter={(value) => {
                const date = new Date(value);
                return `${date.getMonth() + 1}/${date.getDate()}`;
              }}
            />
            <YAxis 
              yAxisId="left"
              tick={{ fill: '#6b7280', fontSize: 12 }}
              tickFormatter={formatNumber}
            />
            <YAxis 
              yAxisId="right"
              orientation="right"
              tick={{ fill: '#6b7280', fontSize: 12 }}
              tickFormatter={formatNumber}
            />
            <Tooltip 
              contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '0.5rem' }}
              formatter={(value: any, name: string) => {
                if (name === 'clicks' || name === 'impressions') {
                  return formatNumber(Number(value));
                }
                if (name === 'ctr') {
                  return formatPercentage(Number(value));
                }
                if (name === 'position') {
                  return formatPosition(Number(value));
                }
                return value;
              }}
              labelFormatter={(label) => {
                const date = new Date(label);
                return date.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
              }}
            />
            <Legend />
            <Area 
              yAxisId="right"
              type="monotone" 
              dataKey="impressions" 
              fill="#e0e7ff" 
              stroke="#8b5cf6" 
              strokeWidth={2}
              name="Impressions"
            />
            <Line 
              yAxisId="left"
              type="monotone" 
              dataKey="clicks" 
              stroke="#3b82f6" 
              strokeWidth={2}
              dot={false}
              name="Clicks"
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      {/* Change Points Section */}
      {change_points && change_points.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Significant Changes</h3>
          <div className="space-y-3">
            {change_points.map((cp, idx) => (
              <div key={idx} className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
                <div className="flex items-center space-x-3">
                  <Calendar className="w-5 h-5 text-gray-500" />
                  <div>
                    <p className="font-medium text-gray-900">
                      {new Date(cp.date).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })}
                    </p>
                    <p className="text-sm text-gray-600">
                      {cp.direction === 'drop' ? 'Traffic Drop' : 'Traffic Spike'}
                    </p>
                  </div>
                </div>
                <div className={`text-lg font-semibold ${cp.direction === 'drop' ? 'text-red-600' : 'text-green-600'}`}>
                  {cp.magnitude > 0 ? '+' : ''}{formatPercentage(cp.magnitude * 100)}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Seasonality Insights */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Seasonality Insights</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="space-y-3">
            <div className="flex items-center justify-between p-4 bg-green-50 rounded-lg">
              <span className="text-sm font-medium text-gray-700">Best Day</span>
              <span className="text-lg font-semibold text-green-700">{seasonality.best_day}</span>
            </div>
            <div className="flex items-center justify-between p-4 bg-red-50 rounded-lg">
              <span className="text-sm font-medium text-gray-700">Worst Day</span>
              <span className="text-lg font-semibold text-red-700">{seasonality.worst_day}</span>
            </div>
          </div>
          <div className="flex flex-col justify-center">
            {seasonality.monthly_cycle && (
              <div className="p-4 bg-blue-50 rounded-lg">
                <p className="text-sm font-medium text-gray-700 mb-1">Monthly Pattern</p>
                <p className="text-sm text-gray-600">{seasonality.cycle_description}</p>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Forecast Section */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Forecast</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="p-4 bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg">
            <p className="text-sm font-medium text-gray-700 mb-2">30 Days</p>
            <p className="text-2xl font-bold text-blue-900">{formatNumber(forecast['30d'].clicks)}</p>
            <p className="text-xs text-gray-600 mt-1">
              {formatNumber(forecast['30d'].ci_low)} - {formatNumber(forecast['30d'].ci_high)} clicks
            </p>
          </div>
          <div className="p-4 bg-gradient-to-br from-purple-50 to-purple-100 rounded-lg">
            <p className="text-sm font-medium text-gray-700 mb-2">60 Days</p>
            <p className="text-2xl font-bold text-purple-900">{formatNumber(forecast['60d'].clicks)}</p>
            <p className="text-xs text-gray-600 mt-1">
              {formatNumber(forecast['60d'].ci_low)} - {formatNumber(forecast['60d'].ci_high)} clicks
            </p>
          </div>
          <div className="p-4 bg-gradient-to-br from-indigo-50 to-indigo-100 rounded-lg">
            <p className="text-sm font-medium text-gray-700 mb-2">90 Days</p>
            <p className="text-2xl font-bold text-indigo-900">{formatNumber(forecast['90d'].clicks)}</p>
            <p className="text-xs text-gray-600 mt-1">
              {formatNumber(forecast['90d'].ci_low)} - {formatNumber(forecast['90d'].ci_high)} clicks
            </p>
          </div>
        </div>
      </div>

      {/* Anomalies Section */}
      {anomalies && anomalies.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Anomalies Detected</h3>
          <div className="space-y-3">
            {anomalies.slice(0, 5).map((anomaly, idx) => (
              <div key={idx} className="flex items-center justify-between p-4 bg-yellow-50 rounded-lg border border-yellow-200">
                <div className="flex items-center space-x-3">
                  <AlertCircle className="w-5 h-5 text-yellow-600" />
                  <div>
                    <p className="font-medium text-gray-900">
                      {new Date(anomaly.date).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })}
                    </p>
                    <p className="text-sm text-gray-600">
                      {anomaly.type === 'discord' ? 'One-off event' : 'Recurring pattern'}
                    </p>
                  </div>
                </div>
                <div className="text-lg font-semibold text-yellow-700">
                  {anomaly.magnitude > 0 ? '+' : ''}{formatPercentage(anomaly.magnitude * 100)}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Device Breakdown */}
      {device_breakdown && device_breakdown.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic by Device</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={device_breakdown}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ device, percentage }) => `${device}: ${formatPercentage(percentage)}`}
                  outerRadius={100}
                  fill="#8884d8"
                  dataKey="clicks"
                >
                  {device_breakdown.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={DEVICE_COLORS[index % DEVICE_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip formatter={(value: any) => formatNumber(Number(value))} />
              </PieChart>
            </ResponsiveContainer>
            <div className="space-y-3">
              {device_breakdown.map((device, idx) => (
                <div key={idx} className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
                  <div className="flex items-center space-x-3">
                    <div 
                      className="w-4 h-4 rounded-full" 
                      style={{ backgroundColor: DEVICE_COLORS[idx % DEVICE_COLORS.length] }}
                    />
                    <span className="font-medium text-gray-900">{device.device}</span>
                  </div>
                  <div className="text-right">
                    <p className="font-semibold text-gray-900">{formatNumber(device.clicks)}</p>
                    <p className="text-xs text-gray-600">{formatPercentage(device.percentage)}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Top Pages */}
      {top_pages && top_pages.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Top Performing Pages</h3>
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Page
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Clicks
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Impressions
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    CTR
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Position
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Change
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {top_pages.slice(0, 10).map((page, idx) => (
                  <tr key={idx} className="hover:bg-gray-50">
                    <td className="px-6 py-4 text-sm text-gray-900 max-w-xs truncate">
                      {page.url}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-900 text-right font-medium">
                      {formatNumber(page.clicks)}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-600 text-right">
                      {formatNumber(page.impressions)}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-600 text-right">
                      {formatPercentage(page.ctr)}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-600 text-right">
                      {formatPosition(page.position)}
                    </td>
                    <td className="px-6 py-4 text-sm text-right">
                      {getChangeIndicator(page.clicks_change_pct)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
};

export default Module1TrafficOverview;
