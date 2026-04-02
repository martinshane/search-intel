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

  const formatPercent = (num: number): string => {
    return (num * 100).toFixed(2) + '%';
  };

  const formatDate = (dateStr: string): string => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  const getDirectionIcon = (direction: string) => {
    switch (direction) {
      case 'strong_growth':
      case 'growth':
        return <TrendingUp className="w-5 h-5 text-green-500" />;
      case 'strong_decline':
      case 'decline':
        return <TrendingDown className="w-5 h-5 text-red-500" />;
      default:
        return <Minus className="w-5 h-5 text-gray-500" />;
    }
  };

  const getDirectionColor = (direction: string): string => {
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

  const getDirectionLabel = (direction: string): string => {
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
        return 'Strong Decline';
      default:
        return 'Unknown';
    }
  };

  const getChangeColor = (change: number): string => {
    if (change > 0) return 'text-green-600';
    if (change < 0) return 'text-red-600';
    return 'text-gray-600';
  };

  const getPositionChangeColor = (change: number): string => {
    // Position change is inverse - negative is good (moved up)
    if (change < 0) return 'text-green-600';
    if (change > 0) return 'text-red-600';
    return 'text-gray-600';
  };

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
        <div className="flex items-start">
          <AlertCircle className="w-5 h-5 text-red-500 mt-0.5 mr-3" />
          <div>
            <h3 className="text-red-800 font-semibold">Error Loading Module</h3>
            <p className="text-red-700 mt-1">{error}</p>
          </div>
        </div>
      </div>
    );
  }

  if (!module1) {
    return (
      <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6">
        <div className="flex items-start">
          <AlertCircle className="w-5 h-5 text-yellow-500 mt-0.5 mr-3" />
          <div>
            <h3 className="text-yellow-800 font-semibold">No Data Available</h3>
            <p className="text-yellow-700 mt-1">Module data is not yet available for this report.</p>
          </div>
        </div>
      </div>
    );
  }

  const { traffic_overview, summary } = module1;
  const timeseries = traffic_overview?.timeseries || [];

  // Prepare chart data
  const chartData = timeseries.map(point => ({
    date: formatDate(point.date),
    fullDate: point.date,
    clicks: point.clicks,
    impressions: point.impressions,
    ctr: point.ctr * 100,
    position: point.position,
  }));

  // Prepare forecast data
  const forecastData = [
    { period: '30 Days', clicks: module1.forecast['30d'].clicks, low: module1.forecast['30d'].ci_low, high: module1.forecast['30d'].ci_high },
    { period: '60 Days', clicks: module1.forecast['60d'].clicks, low: module1.forecast['60d'].ci_low, high: module1.forecast['60d'].ci_high },
    { period: '90 Days', clicks: module1.forecast['90d'].clicks, low: module1.forecast['90d'].ci_low, high: module1.forecast['90d'].ci_high },
  ];

  // Device breakdown colors
  const DEVICE_COLORS: Record<string, string> = {
    desktop: '#3B82F6',
    mobile: '#10B981',
    tablet: '#F59E0B',
  };

  return (
    <div className="space-y-8">
      {/* Header Section */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-2xl font-bold text-gray-900">Traffic Overview & Health</h2>
            <p className="text-gray-600 mt-1">Comprehensive analysis of your organic search performance</p>
          </div>
          <div className="flex items-center space-x-3">
            {getDirectionIcon(module1.overall_direction)}
            <div className="text-right">
              <div className={`text-lg font-semibold ${getDirectionColor(module1.overall_direction)}`}>
                {getDirectionLabel(module1.overall_direction)}
              </div>
              <div className="text-sm text-gray-600">
                {module1.trend_slope_pct_per_month > 0 ? '+' : ''}{module1.trend_slope_pct_per_month.toFixed(1)}% per month
              </div>
            </div>
          </div>
        </div>

        {/* Key Metrics Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {/* Total Clicks */}
          <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-gray-600">Total Clicks</h3>
              <Activity className="w-4 h-4 text-gray-400" />
            </div>
            <div className="text-2xl font-bold text-gray-900">{formatNumber(summary.total_clicks)}</div>
            <div className={`text-sm mt-1 ${getChangeColor(summary.clicks_change_pct)}`}>
              {summary.clicks_change_pct > 0 ? '+' : ''}{summary.clicks_change_pct.toFixed(1)}% vs previous period
            </div>
          </div>

          {/* Total Impressions */}
          <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-gray-600">Total Impressions</h3>
              <Activity className="w-4 h-4 text-gray-400" />
            </div>
            <div className="text-2xl font-bold text-gray-900">{formatNumber(summary.total_impressions)}</div>
            <div className={`text-sm mt-1 ${getChangeColor(summary.impressions_change_pct)}`}>
              {summary.impressions_change_pct > 0 ? '+' : ''}{summary.impressions_change_pct.toFixed(1)}% vs previous period
            </div>
          </div>

          {/* Average CTR */}
          <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-gray-600">Average CTR</h3>
              <Activity className="w-4 h-4 text-gray-400" />
            </div>
            <div className="text-2xl font-bold text-gray-900">{formatPercent(summary.avg_ctr)}</div>
            <div className={`text-sm mt-1 ${getChangeColor(summary.ctr_change_pct)}`}>
              {summary.ctr_change_pct > 0 ? '+' : ''}{summary.ctr_change_pct.toFixed(1)}% vs previous period
            </div>
          </div>

          {/* Average Position */}
          <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-gray-600">Average Position</h3>
              <Activity className="w-4 h-4 text-gray-400" />
            </div>
            <div className="text-2xl font-bold text-gray-900">{summary.avg_position.toFixed(1)}</div>
            <div className={`text-sm mt-1 ${getPositionChangeColor(summary.position_change)}`}>
              {summary.position_change > 0 ? '+' : ''}{summary.position_change.toFixed(1)} vs previous period
            </div>
          </div>
        </div>
      </div>

      {/* Traffic Trends Chart */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Trends Over Time</h3>
        <ResponsiveContainer width="100%" height={400}>
          <ComposedChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis 
              dataKey="date" 
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
            />
            <Tooltip 
              contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '6px' }}
              formatter={(value: number, name: string) => {
                if (name === 'clicks' || name === 'impressions') {
                  return [formatNumber(value), name.charAt(0).toUpperCase() + name.slice(1)];
                }
                return [value.toFixed(2), name.charAt(0).toUpperCase() + name.slice(1)];
              }}
            />
            <Legend />
            <Area 
              yAxisId="left"
              type="monotone" 
              dataKey="impressions" 
              fill="#93c5fd" 
              stroke="#3b82f6"
              fillOpacity={0.3}
              name="Impressions"
            />
            <Line 
              yAxisId="left"
              type="monotone" 
              dataKey="clicks" 
              stroke="#10b981"
              strokeWidth={2}
              dot={false}
              name="Clicks"
            />
            <Line 
              yAxisId="right"
              type="monotone" 
              dataKey="position" 
              stroke="#f59e0b"
              strokeWidth={2}
              dot={false}
              name="Avg Position"
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      {/* CTR Trend Chart */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Click-Through Rate Trend</h3>
        <ResponsiveContainer width="100%" height={300}>
          <AreaChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis 
              dataKey="date" 
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
            />
            <YAxis 
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
              tickFormatter={(value) => value.toFixed(1) + '%'}
            />
            <Tooltip 
              contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '6px' }}
              formatter={(value: number) => [value.toFixed(2) + '%', 'CTR']}
            />
            <Area 
              type="monotone" 
              dataKey="ctr" 
              fill="#8b5cf6" 
              stroke="#7c3aed"
              fillOpacity={0.5}
              name="CTR %"
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Seasonality and Change Points */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Seasonality Insights */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center mb-4">
            <Calendar className="w-5 h-5 text-blue-600 mr-2" />
            <h3 className="text-lg font-semibold text-gray-900">Seasonality Patterns</h3>
          </div>
          <div className="space-y-4">
            <div>
              <div className="text-sm font-medium text-gray-600 mb-1">Best Day of Week</div>
              <div className="text-xl font-bold text-green-600">{module1.seasonality.best_day}</div>
            </div>
            <div>
              <div className="text-sm font-medium text-gray-600 mb-1">Worst Day of Week</div>
              <div className="text-xl font-bold text-red-600">{module1.seasonality.worst_day}</div>
            </div>
            {module1.seasonality.monthly_cycle && (
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 mt-4">
                <div className="text-sm font-medium text-blue-900 mb-1">Monthly Cycle Detected</div>
                <div className="text-sm text-blue-700">{module1.seasonality.cycle_description}</div>
              </div>
            )}
          </div>
        </div>

        {/* Change Points */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center mb-4">
            <AlertCircle className="w-5 h-5 text-orange-600 mr-2" />
            <h3 className="text-lg font-semibold text-gray-900">Significant Changes</h3>
          </div>
          <div className="space-y-3">
            {module1.change_points.length === 0 ? (
              <p className="text-gray-600 text-sm">No significant change points detected in the analysis period.</p>
            ) : (
              module1.change_points.slice(0, 5).map((cp, idx) => (
                <div key={idx} className="flex items-center justify-between border-b border-gray-200 pb-2 last:border-b-0">
                  <div className="flex items-center">
                    {cp.direction === 'drop' ? (
                      <TrendingDown className="w-4 h-4 text-red-500 mr-2" />
                    ) : (
                      <TrendingUp className="w-4 h-4 text-green-500 mr-2" />
                    )}
                    <div>
                      <div className="text-sm font-medium text-gray-900">
                        {new Date(cp.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}
                      </div>
                      <div className="text-xs text-gray-600">{cp.direction === 'drop' ? 'Traffic Drop' : 'Traffic Spike'}</div>
                    </div>
                  </div>
                  <div className={`text-sm font-semibold ${cp.direction === 'drop' ? 'text-red-600' : 'text-green-600'}`}>
                    {cp.magnitude > 0 ? '+' : ''}{(cp.magnitude * 100).toFixed(1)}%
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>

      {/* Forecast */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Forecast</h3>
        <p className="text-gray-600 text-sm mb-6">Projected organic clicks based on historical trends and seasonality patterns</p>
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart data={forecastData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis 
              dataKey="period" 
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
            />
            <YAxis 
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
              tickFormatter={formatNumber}
            />
            <Tooltip 
              contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '6px' }}
              formatter={(value: number, name: string) => {
                const labels: Record<string, string> = {
                  clicks: 'Projected Clicks',
                  low: 'Lower Bound',
                  high: 'Upper Bound'
                };
                return [formatNumber(value), labels[name] || name];
              }}
            />
            <Legend />
            <Area 
              type="monotone" 
              dataKey="high" 
              fill="#93c5fd" 
              stroke="none"
              fillOpacity={0.2}
              name="Upper Bound"
            />
            <Area 
              type="monotone" 
              dataKey="low" 
              fill="#93c5fd" 
              stroke="none"
              fillOpacity={0.2}
              name="Lower Bound"
            />
            <Line 
              type="monotone" 
              dataKey="clicks" 
              stroke="#3b82f6"
              strokeWidth={3}
              dot={{ fill: '#3b82f6', r: 6 }}
              name="Projected Clicks"
            />
          </ComposedChart>
        </ResponsiveContainer>
        <div className="grid grid-cols-3 gap-4 mt-6">
          {forecastData.map((forecast, idx) => (
            <div key={idx} className="bg-gray-50 rounded-lg p-4 border border-gray-200">
              <div className="text-sm font-medium text-gray-600 mb-1">{forecast.period}</div>
              <div className="text-xl font-bold text-gray-900">{formatNumber(forecast.clicks)}</div>
              <div className="text-xs text-gray-600 mt-1">
                Range: {formatNumber(forecast.low)} - {formatNumber(forecast.high)}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Anomalies */}
      {module1.anomalies.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Detected Anomalies</h3>
          <div className="space-y-3">
            {module1.anomalies.map((anomaly, idx) => (
              <div key={idx} className="flex items-center justify-between border-l-4 border-orange-500 pl-4 py-2 bg-orange-50">
                <div className="flex items-center">
                  <AlertCircle className="w-5 h-5 text-orange-600 mr-3" />
                  <div>
                    <div className="text-sm font-medium text-gray-900">
                      {new Date(anomaly.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}
                    </div>
                    <div className="text-xs text-gray-600">
                      {anomaly.type === 'discord' ? 'One-off anomaly' : 'Recurring pattern'}
                    </div>
                  </div>
                </div>
                <div className={`text-sm font-semibold ${anomaly.magnitude < 0 ? 'text-red-600' : 'text-orange-600'}`}>
                  {anomaly.magnitude > 0 ? '+' : ''}{(anomaly.magnitude * 100).toFixed(1)}%
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Device Breakdown */}
      {module1.device_breakdown && module1.device_breakdown.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Device Breakdown</h3>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div>
              <ResponsiveContainer width="100%" height={250}>
                <PieChart>
                  <Pie
                    data={module1.device_breakdown}
                    dataKey="clicks"
                    nameKey="device"
                    cx="50%"
                    cy="50%"
                    outerRadius={80}
                    label={({ device, percentage }) => `${device}: ${percentage.toFixed(1)}%`}
                  >
                    {module1.device_breakdown.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={DEVICE_COLORS[entry.device.toLowerCase()] || '#94a3b8'} />
                    ))}
                  </Pie>
                  <Tooltip 
                    formatter={(value: number) => formatNumber(value)}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="space-y-3">
              {module1.device_breakdown.map((device, idx) => (
                <div key={idx} className="border border-gray-200 rounded-lg p-4">
                  <div className="flex items-center justify-between mb-2">
                    <div className="text-sm font-semibold text-gray-900 capitalize">{device.device}</div>
                    <div className="text-sm font-medium text-gray-600">{device.percentage.toFixed(1)}%</div>
                  </div>
                  <div className="grid grid-cols-2 gap-4 text-xs">
                    <div>
                      <div className="text-gray-600">Clicks</div>
                      <div className="font-semibold text-gray-900">{formatNumber(device.clicks)}</div>
                    </div>
                    <div>
                      <div className="text-gray-600">Impressions</div>
                      <div className="font-semibold text-gray-900">{formatNumber(device.impressions)}</div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Top Pages */}
      {module1.top_pages && module1.top_pages.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Top Performing Pages</h3>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left text-xs font-medium text-gray-600 uppercase py-3 px-4">Page</th>
                  <th className="text-right text-xs font-medium text-gray-600 uppercase py-3 px-4">Clicks</th>
                  <th className="text-right text-xs font-medium text-gray-600 uppercase py-3 px-4">Impressions</th>
                  <th className="text-right text-xs font-medium text-gray-600 uppercase py-3 px-4">CTR</th>
                  <th className="text-right text-xs font-medium text-gray-600 uppercase py-3 px-4">Position</th>
                  <th className="text-right text-xs font-medium text-gray-600 uppercase py-3 px-4">Change</th>
                </tr>
              </thead>
              <tbody>
                {module1.top_pages.slice(0, 10).map((page, idx) => (
                  <tr key={idx} className="border-b border-gray-100 hover:bg-gray-50">
                    <td className="py-3 px-4 text-sm text-gray-900 max-w-md truncate" title={page.url}>
                      {page.url}
                    </td>
                    <td className="py-3 px-4 text-sm text-gray-900 text-right font-medium">
                      {formatNumber(page.clicks)}
                    </td>
                    <td className="py-3 px-4 text-sm text-gray-600 text-right">
                      {formatNumber(page.impressions)}
                    </td>
                    <td className="py-3 px-4 text-sm text-gray-600 text-right">
                      {formatPercent(page.ctr)}
                    </td>
                    <td className="py-3 px-4 text-sm text-gray-600 text-right">
                      {page.position.toFixed(1)}
                    </td>
                    <td className={`py-3 px-4 text-sm text-right font-medium ${getChangeColor(page.clicks_change_pct)}`}>
                      {page.clicks_change_pct > 0 ? '+' : ''}{page.clicks_change_pct.toFixed(1)}%
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
