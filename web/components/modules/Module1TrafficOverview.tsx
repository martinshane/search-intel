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
import { TrendingUp, TrendingDown, Minus, AlertCircle, Calendar, Activity, Monitor, Smartphone, Tablet } from 'lucide-react';

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
    return (num * 100).toFixed(1) + '%';
  };

  const formatDate = (dateString: string): string => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  const getTrendIcon = (direction: string) => {
    switch (direction) {
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

  const getTrendColor = (direction: string): string => {
    switch (direction) {
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

  const getTrendLabel = (direction: string): string => {
    switch (direction) {
      case 'strong_growth':
        return 'Strong Growth';
      case 'growth':
        return 'Growing';
      case 'flat':
        return 'Stable';
      case 'decline':
        return 'Declining';
      case 'strong_decline':
        return 'Sharp Decline';
      default:
        return 'Unknown';
    }
  };

  const getChangeColor = (change: number): string => {
    if (change > 0) return 'text-green-600';
    if (change < 0) return 'text-red-600';
    return 'text-gray-600';
  };

  const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'];

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-200 rounded w-1/3 mb-6"></div>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="h-32 bg-gray-200 rounded"></div>
            ))}
          </div>
          <div className="h-96 bg-gray-200 rounded mb-6"></div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
            <div className="h-64 bg-gray-200 rounded"></div>
            <div className="h-64 bg-gray-200 rounded"></div>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="flex items-center gap-3 text-red-600">
          <AlertCircle className="w-6 h-6" />
          <div>
            <h3 className="font-semibold text-lg">Error Loading Data</h3>
            <p className="text-sm text-red-500">{error}</p>
          </div>
        </div>
      </div>
    );
  }

  if (!module1) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="flex items-center gap-3 text-gray-600">
          <AlertCircle className="w-6 h-6" />
          <p>No data available</p>
        </div>
      </div>
    );
  }

  const { traffic_overview, overall_direction, trend_slope_pct_per_month, change_points, seasonality, anomalies, forecast, top_pages, device_breakdown } = module1;

  const getKeyInsights = (): string[] => {
    const insights: string[] = [];

    // Overall trend insight
    const trendLabel = getTrendLabel(overall_direction);
    const slopeAbs = Math.abs(trend_slope_pct_per_month);
    insights.push(
      `Your traffic is ${trendLabel.toLowerCase()} at ${slopeAbs.toFixed(1)}% per month (${trend_slope_pct_per_month > 0 ? '+' : ''}${trend_slope_pct_per_month.toFixed(1)}%/mo)`
    );

    // Seasonality insight
    if (seasonality.monthly_cycle) {
      insights.push(`Seasonal pattern detected: ${seasonality.cycle_description}`);
    }
    insights.push(`Best performing day: ${seasonality.best_day}, Worst: ${seasonality.worst_day}`);

    // Change points
    if (change_points.length > 0) {
      const mostRecent = change_points[change_points.length - 1];
      const direction = mostRecent.direction === 'drop' ? 'dropped' : 'spiked';
      const magnitude = Math.abs(mostRecent.magnitude * 100).toFixed(0);
      insights.push(
        `Traffic ${direction} ${magnitude}% on ${formatDate(mostRecent.date)}`
      );
    }

    // Anomalies
    const recentAnomalies = anomalies.filter(a => {
      const anomalyDate = new Date(a.date);
      const thirtyDaysAgo = new Date();
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
      return anomalyDate >= thirtyDaysAgo;
    });
    if (recentAnomalies.length > 0) {
      insights.push(`${recentAnomalies.length} traffic anomalies detected in the last 30 days`);
    }

    // Forecast
    const forecast30d = forecast['30d'];
    const currentClicks = traffic_overview.summary.total_clicks;
    const projectedChange = ((forecast30d.clicks - currentClicks) / currentClicks * 100);
    if (Math.abs(projectedChange) > 5) {
      insights.push(
        `30-day forecast: ${projectedChange > 0 ? '+' : ''}${projectedChange.toFixed(0)}% clicks (${formatNumber(forecast30d.clicks)} ±${formatNumber(forecast30d.ci_high - forecast30d.clicks)})`
      );
    }

    // Device breakdown insight
    if (device_breakdown && device_breakdown.length > 0) {
      const topDevice = device_breakdown.reduce((prev, current) => 
        (current.clicks > prev.clicks) ? current : prev
      );
      insights.push(`${topDevice.device} drives ${topDevice.percentage.toFixed(0)}% of your search traffic`);
    }

    return insights;
  };

  return (
    <div className="bg-white rounded-lg shadow-md p-6 space-y-8">
      {/* Header */}
      <div className="border-b pb-4">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-2xl font-bold text-gray-900">Traffic Overview & Health</h2>
            <p className="text-sm text-gray-600 mt-1">90-day trend analysis, trajectory, and performance metrics</p>
          </div>
          <div className={`flex items-center gap-2 px-4 py-2 rounded-lg bg-gray-50 ${getTrendColor(overall_direction)}`}>
            {getTrendIcon(overall_direction)}
            <span className="font-semibold">{getTrendLabel(overall_direction)}</span>
          </div>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg p-4 border border-blue-200">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-blue-900">Total Clicks</span>
            <Activity className="w-5 h-5 text-blue-600" />
          </div>
          <div className="text-2xl font-bold text-blue-900">
            {formatNumber(traffic_overview.summary.total_clicks)}
          </div>
          <div className={`text-sm mt-1 ${getChangeColor(traffic_overview.summary.clicks_change_pct)}`}>
            {traffic_overview.summary.clicks_change_pct > 0 ? '+' : ''}
            {traffic_overview.summary.clicks_change_pct.toFixed(1)}% vs prev period
          </div>
        </div>

        <div className="bg-gradient-to-br from-green-50 to-green-100 rounded-lg p-4 border border-green-200">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-green-900">Total Impressions</span>
            <Activity className="w-5 h-5 text-green-600" />
          </div>
          <div className="text-2xl font-bold text-green-900">
            {formatNumber(traffic_overview.summary.total_impressions)}
          </div>
          <div className={`text-sm mt-1 ${getChangeColor(traffic_overview.summary.impressions_change_pct)}`}>
            {traffic_overview.summary.impressions_change_pct > 0 ? '+' : ''}
            {traffic_overview.summary.impressions_change_pct.toFixed(1)}% vs prev period
          </div>
        </div>

        <div className="bg-gradient-to-br from-purple-50 to-purple-100 rounded-lg p-4 border border-purple-200">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-purple-900">Average CTR</span>
            <Activity className="w-5 h-5 text-purple-600" />
          </div>
          <div className="text-2xl font-bold text-purple-900">
            {formatPercentage(traffic_overview.summary.avg_ctr)}
          </div>
          <div className={`text-sm mt-1 ${getChangeColor(traffic_overview.summary.ctr_change_pct)}`}>
            {traffic_overview.summary.ctr_change_pct > 0 ? '+' : ''}
            {traffic_overview.summary.ctr_change_pct.toFixed(1)}% vs prev period
          </div>
        </div>

        <div className="bg-gradient-to-br from-orange-50 to-orange-100 rounded-lg p-4 border border-orange-200">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-orange-900">Average Position</span>
            <Activity className="w-5 h-5 text-orange-600" />
          </div>
          <div className="text-2xl font-bold text-orange-900">
            {traffic_overview.summary.avg_position.toFixed(1)}
          </div>
          <div className={`text-sm mt-1 ${getChangeColor(-traffic_overview.summary.position_change)}`}>
            {traffic_overview.summary.position_change < 0 ? '+' : ''}
            {Math.abs(traffic_overview.summary.position_change).toFixed(1)} positions
          </div>
        </div>
      </div>

      {/* 90-Day Trend Chart */}
      <div>
        <h3 className="text-lg font-semibold text-gray-900 mb-4">90-Day Performance Trend</h3>
        <ResponsiveContainer width="100%" height={400}>
          <ComposedChart data={traffic_overview.timeseries}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis 
              dataKey="date" 
              tickFormatter={formatDate}
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
            />
            <YAxis 
              yAxisId="left"
              stroke="#3b82f6"
              style={{ fontSize: '12px' }}
              tickFormatter={formatNumber}
            />
            <YAxis 
              yAxisId="right"
              orientation="right"
              stroke="#10b981"
              style={{ fontSize: '12px' }}
              tickFormatter={formatNumber}
            />
            <Tooltip 
              contentStyle={{ 
                backgroundColor: 'rgba(255, 255, 255, 0.95)', 
                border: '1px solid #e5e7eb',
                borderRadius: '8px',
                padding: '12px'
              }}
              formatter={(value: number, name: string) => {
                if (name === 'ctr') return [formatPercentage(value), 'CTR'];
                if (name === 'position') return [value.toFixed(1), 'Position'];
                return [formatNumber(value), name];
              }}
              labelFormatter={(label) => formatDate(label)}
            />
            <Legend />
            <Area
              yAxisId="right"
              type="monotone"
              dataKey="impressions"
              fill="#10b981"
              stroke="#10b981"
              fillOpacity={0.2}
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

      {/* Top Pages Table */}
      {top_pages && top_pages.length > 0 && (
        <div>
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
                {top_pages.slice(0, 10).map((page, index) => (
                  <tr key={index} className="hover:bg-gray-50">
                    <td className="px-6 py-4 text-sm text-gray-900 max-w-md truncate" title={page.url}>
                      {page.url}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-900 text-right font-medium">
                      {formatNumber(page.clicks)}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-600 text-right">
                      {formatNumber(page.impressions)}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-900 text-right">
                      {formatPercentage(page.ctr)}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-900 text-right">
                      {page.position.toFixed(1)}
                    </td>
                    <td className={`px-6 py-4 text-sm text-right font-medium ${getChangeColor(page.clicks_change_pct)}`}>
                      {page.clicks_change_pct > 0 ? '+' : ''}
                      {page.clicks_change_pct.toFixed(0)}%
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Device Breakdown & Forecast */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Device Breakdown Pie Chart */}
        {device_breakdown && device_breakdown.length > 0 && (
          <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Device Breakdown</h3>
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={device_breakdown}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ device, percentage }) => `${device}: ${percentage.toFixed(0)}%`}
                  outerRadius={100}
                  fill="#8884d8"
                  dataKey="clicks"
                >
                  {device_breakdown.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip 
                  formatter={(value: number) => formatNumber(value)}
                  contentStyle={{ 
                    backgroundColor: 'rgba(255, 255, 255, 0.95)', 
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px'
                  }}
                />
              </PieChart>
            </ResponsiveContainer>
            <div className="mt-4 space-y-2">
              {device_breakdown.map((device, index) => (
                <div key={index} className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    {device.device === 'DESKTOP' && <Monitor className="w-4 h-4 text-gray-600" />}
                    {device.device === 'MOBILE' && <Smartphone className="w-4 h-4 text-gray-600" />}
                    {device.device === 'TABLET' && <Tablet className="w-4 h-4 text-gray-600" />}
                    <span className="text-sm font-medium text-gray-700">{device.device}</span>
                  </div>
                  <span className="text-sm text-gray-600">{formatNumber(device.clicks)} clicks</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Forecast */}
        <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <Calendar className="w-5 h-5" />
            Traffic Forecast
          </h3>
          <div className="space-y-4">
            {(['30d', '60d', '90d'] as const).map((period) => {
              const forecastData = forecast[period];
              const periodLabel = period === '30d' ? '30 Days' : period === '60d' ? '60 Days' : '90 Days';
              const changeFromCurrent = ((forecastData.clicks - traffic_overview.summary.total_clicks) / traffic_overview.summary.total_clicks * 100);
              
              return (
                <div key={period} className="bg-white rounded-lg p-4 border border-gray-200">
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-sm font-medium text-gray-700">{periodLabel}</span>
                    <span className={`text-sm font-semibold ${getChangeColor(changeFromCurrent)}`}>
                      {changeFromCurrent > 0 ? '+' : ''}{changeFromCurrent.toFixed(0)}%
                    </span>
                  </div>
                  <div className="text-2xl font-bold text-gray-900">
                    {formatNumber(forecastData.clicks)}
                  </div>
                  <div className="text-xs text-gray-500 mt-1">
                    Range: {formatNumber(forecastData.ci_low)} - {formatNumber(forecastData.ci_high)}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Key Insights */}
      <div className="bg-blue-50 rounded-lg p-6 border border-blue-200">
        <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
          <AlertCircle className="w-5 h-5 text-blue-600" />
          Key Insights
        </h3>
        <ul className="space-y-2">
          {getKeyInsights().map((insight, index) => (
            <li key={index} className="flex items-start gap-2">
              <span className="text-blue-600 mt-1">•</span>
              <span className="text-sm text-gray-700">{insight}</span>
            </li>
          ))}
        </ul>
      </div>

      {/* Change Points & Anomalies */}
      {(change_points.length > 0 || anomalies.length > 0) && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {change_points.length > 0 && (
            <div>
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Detected Change Points</h3>
              <div className="space-y-2">
                {change_points.slice(-5).reverse().map((cp, index) => (
                  <div key={index} className="bg-gray-50 rounded-lg p-3 border border-gray-200">
                    <div className="flex items-center justify-between">
                      <span className="text-sm font-medium text-gray-700">{formatDate(cp.date)}</span>
                      <span className={`text-sm font-semibold ${cp.direction === 'spike' ? 'text-green-600' : 'text-red-600'}`}>
                        {cp.direction === 'spike' ? '+' : ''}{(cp.magnitude * 100).toFixed(0)}%
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {anomalies.length > 0 && (
            <div>
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Anomalies</h3>
              <div className="space-y-2">
                {anomalies.slice(-5).reverse().map((anomaly, index) => (
                  <div key={index} className="bg-gray-50 rounded-lg p-3 border border-gray-200">
                    <div className="flex items-center justify-between">
                      <div>
                        <span className="text-sm font-medium text-gray-700">{formatDate(anomaly.date)}</span>
                        <span className="ml-2 text-xs text-gray-500">({anomaly.type})</span>
                      </div>
                      <span className="text-sm font-semibold text-gray-900">
                        {(anomaly.magnitude * 100).toFixed(0)}%
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default Module1TrafficOverview;
