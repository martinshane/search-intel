import React, { useEffect, useState } from 'react';
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  BarChart,
  Bar,
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
import { TrendingUp, TrendingDown, Minus, AlertCircle, Calendar, Activity, MapPin, Monitor } from 'lucide-react';

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

interface CountryBreakdown {
  country: string;
  clicks: number;
  impressions: number;
  percentage: number;
}

interface QueryPerformance {
  query: string;
  clicks: number;
  impressions: number;
  ctr: number;
  position: number;
  clicks_change_pct: number;
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
  country_breakdown?: CountryBreakdown[];
  query_performance?: QueryPerformance[];
}

interface Module1TrafficOverviewProps {
  reportId: string;
}

const Module1TrafficOverview: React.FC<Module1TrafficOverviewProps> = ({ reportId }) => {
  const [module1, setModule1] = useState<Module1Data | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [chartView, setChartView] = useState<'clicks' | 'impressions' | 'ctr' | 'position'>('clicks');

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

  const formatPosition = (num: number): string => {
    return num.toFixed(1);
  };

  const getDirectionIcon = (direction: string) => {
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
    const labels: { [key: string]: string } = {
      strong_growth: 'Strong Growth',
      growth: 'Growth',
      flat: 'Stable',
      decline: 'Declining',
      strong_decline: 'Strong Decline',
    };
    return labels[direction] || direction;
  };

  const getChangeColor = (change: number): string => {
    if (change > 5) return 'text-green-600';
    if (change < -5) return 'text-red-600';
    return 'text-gray-600';
  };

  const DEVICE_COLORS = {
    desktop: '#3b82f6',
    mobile: '#10b981',
    tablet: '#f59e0b',
  };

  const COUNTRY_COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16'];

  if (loading) {
    return (
      <div className="w-full py-12 flex justify-center items-center">
        <div className="flex flex-col items-center space-y-4">
          <Activity className="w-8 h-8 animate-spin text-blue-600" />
          <p className="text-gray-600">Loading traffic overview...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="w-full py-12">
        <div className="bg-red-50 border border-red-200 rounded-lg p-6 flex items-start space-x-3">
          <AlertCircle className="w-6 h-6 text-red-600 flex-shrink-0 mt-0.5" />
          <div>
            <h3 className="text-red-800 font-semibold mb-1">Error Loading Data</h3>
            <p className="text-red-700">{error}</p>
          </div>
        </div>
      </div>
    );
  }

  if (!module1) {
    return (
      <div className="w-full py-12">
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6 flex items-start space-x-3">
          <AlertCircle className="w-6 h-6 text-yellow-600 flex-shrink-0 mt-0.5" />
          <div>
            <h3 className="text-yellow-800 font-semibold mb-1">No Data Available</h3>
            <p className="text-yellow-700">Traffic overview data is not available for this report.</p>
          </div>
        </div>
      </div>
    );
  }

  const { traffic_overview, summary } = module1;
  const timeseries = traffic_overview?.timeseries || [];

  return (
    <div className="w-full space-y-8">
      {/* Header Section */}
      <div className="border-b pb-6">
        <h2 className="text-3xl font-bold text-gray-900 mb-2">Traffic Overview & Health</h2>
        <p className="text-gray-600">
          Comprehensive analysis of your search traffic patterns, trends, and forecasts
        </p>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="bg-white border rounded-lg p-6 shadow-sm">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-gray-600">Total Clicks</span>
            <Activity className="w-5 h-5 text-blue-600" />
          </div>
          <div className="text-3xl font-bold text-gray-900 mb-1">
            {formatNumber(summary.total_clicks)}
          </div>
          <div className={`text-sm font-medium ${getChangeColor(summary.clicks_change_pct)}`}>
            {summary.clicks_change_pct > 0 ? '+' : ''}
            {summary.clicks_change_pct.toFixed(1)}% vs previous period
          </div>
        </div>

        <div className="bg-white border rounded-lg p-6 shadow-sm">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-gray-600">Total Impressions</span>
            <Activity className="w-5 h-5 text-green-600" />
          </div>
          <div className="text-3xl font-bold text-gray-900 mb-1">
            {formatNumber(summary.total_impressions)}
          </div>
          <div className={`text-sm font-medium ${getChangeColor(summary.impressions_change_pct)}`}>
            {summary.impressions_change_pct > 0 ? '+' : ''}
            {summary.impressions_change_pct.toFixed(1)}% vs previous period
          </div>
        </div>

        <div className="bg-white border rounded-lg p-6 shadow-sm">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-gray-600">Average CTR</span>
            <Activity className="w-5 h-5 text-purple-600" />
          </div>
          <div className="text-3xl font-bold text-gray-900 mb-1">
            {formatPercent(summary.avg_ctr)}
          </div>
          <div className={`text-sm font-medium ${getChangeColor(summary.ctr_change_pct)}`}>
            {summary.ctr_change_pct > 0 ? '+' : ''}
            {summary.ctr_change_pct.toFixed(1)}% vs previous period
          </div>
        </div>

        <div className="bg-white border rounded-lg p-6 shadow-sm">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-gray-600">Average Position</span>
            <Activity className="w-5 h-5 text-orange-600" />
          </div>
          <div className="text-3xl font-bold text-gray-900 mb-1">
            {formatPosition(summary.avg_position)}
          </div>
          <div className={`text-sm font-medium ${getChangeColor(-summary.position_change)}`}>
            {summary.position_change < 0 ? '+' : ''}
            {Math.abs(summary.position_change).toFixed(1)} positions
          </div>
        </div>
      </div>

      {/* Overall Direction Card */}
      <div className="bg-gradient-to-br from-blue-50 to-indigo-50 border border-blue-200 rounded-lg p-6">
        <div className="flex items-start space-x-4">
          <div className="flex-shrink-0">
            {getDirectionIcon(module1.overall_direction)}
          </div>
          <div className="flex-grow">
            <h3 className="text-lg font-semibold text-gray-900 mb-2">
              Overall Trend: {getDirectionLabel(module1.overall_direction)}
            </h3>
            <p className="text-gray-700 mb-3">
              Your site is currently {module1.overall_direction === 'strong_decline' || module1.overall_direction === 'decline' ? 'declining' : module1.overall_direction === 'strong_growth' || module1.overall_direction === 'growth' ? 'growing' : 'stable'} at{' '}
              <span className={`font-semibold ${getDirectionColor(module1.overall_direction)}`}>
                {Math.abs(module1.trend_slope_pct_per_month).toFixed(1)}% per month
              </span>
            </p>
            {module1.seasonality && (
              <div className="bg-white rounded-lg p-4 border border-blue-200">
                <h4 className="font-semibold text-gray-900 mb-2 flex items-center">
                  <Calendar className="w-4 h-4 mr-2" />
                  Seasonality Pattern
                </h4>
                <div className="space-y-2 text-sm text-gray-700">
                  <p>
                    <span className="font-medium">Best day:</span> {module1.seasonality.best_day}
                  </p>
                  <p>
                    <span className="font-medium">Worst day:</span> {module1.seasonality.worst_day}
                  </p>
                  {module1.seasonality.monthly_cycle && (
                    <p>
                      <span className="font-medium">Monthly pattern:</span> {module1.seasonality.cycle_description}
                    </p>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Chart View Selector */}
      <div className="bg-white border rounded-lg p-6 shadow-sm">
        <div className="flex items-center justify-between mb-6">
          <h3 className="text-xl font-semibold text-gray-900">Performance Trends</h3>
          <div className="flex space-x-2">
            <button
              onClick={() => setChartView('clicks')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                chartView === 'clicks'
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              Clicks
            </button>
            <button
              onClick={() => setChartView('impressions')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                chartView === 'impressions'
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              Impressions
            </button>
            <button
              onClick={() => setChartView('ctr')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                chartView === 'ctr'
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              CTR
            </button>
            <button
              onClick={() => setChartView('position')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                chartView === 'position'
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              Position
            </button>
          </div>
        </div>

        <ResponsiveContainer width="100%" height={400}>
          {chartView === 'clicks' && (
            <AreaChart data={timeseries}>
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
                fontSize={12}
                tickFormatter={(value) => {
                  const date = new Date(value);
                  return `${date.getMonth() + 1}/${date.getDate()}`;
                }}
              />
              <YAxis stroke="#6b7280" fontSize={12} tickFormatter={formatNumber} />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'white',
                  border: '1px solid #e5e7eb',
                  borderRadius: '8px',
                  boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                }}
                formatter={(value: number) => [formatNumber(value), 'Clicks']}
                labelFormatter={(label) => new Date(label).toLocaleDateString()}
              />
              <Area
                type="monotone"
                dataKey="clicks"
                stroke="#3b82f6"
                strokeWidth={2}
                fillOpacity={1}
                fill="url(#colorClicks)"
              />
            </AreaChart>
          )}
          {chartView === 'impressions' && (
            <AreaChart data={timeseries}>
              <defs>
                <linearGradient id="colorImpressions" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#10b981" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="date"
                stroke="#6b7280"
                fontSize={12}
                tickFormatter={(value) => {
                  const date = new Date(value);
                  return `${date.getMonth() + 1}/${date.getDate()}`;
                }}
              />
              <YAxis stroke="#6b7280" fontSize={12} tickFormatter={formatNumber} />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'white',
                  border: '1px solid #e5e7eb',
                  borderRadius: '8px',
                  boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                }}
                formatter={(value: number) => [formatNumber(value), 'Impressions']}
                labelFormatter={(label) => new Date(label).toLocaleDateString()}
              />
              <Area
                type="monotone"
                dataKey="impressions"
                stroke="#10b981"
                strokeWidth={2}
                fillOpacity={1}
                fill="url(#colorImpressions)"
              />
            </AreaChart>
          )}
          {chartView === 'ctr' && (
            <LineChart data={timeseries}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="date"
                stroke="#6b7280"
                fontSize={12}
                tickFormatter={(value) => {
                  const date = new Date(value);
                  return `${date.getMonth() + 1}/${date.getDate()}`;
                }}
              />
              <YAxis
                stroke="#6b7280"
                fontSize={12}
                tickFormatter={(value) => (value * 100).toFixed(1) + '%'}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'white',
                  border: '1px solid #e5e7eb',
                  borderRadius: '8px',
                  boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                }}
                formatter={(value: number) => [formatPercent(value), 'CTR']}
                labelFormatter={(label) => new Date(label).toLocaleDateString()}
              />
              <Line
                type="monotone"
                dataKey="ctr"
                stroke="#8b5cf6"
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          )}
          {chartView === 'position' && (
            <LineChart data={timeseries}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="date"
                stroke="#6b7280"
                fontSize={12}
                tickFormatter={(value) => {
                  const date = new Date(value);
                  return `${date.getMonth() + 1}/${date.getDate()}`;
                }}
              />
              <YAxis
                stroke="#6b7280"
                fontSize={12}
                reversed
                domain={[1, 'auto']}
                tickFormatter={(value) => value.toFixed(1)}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'white',
                  border: '1px solid #e5e7eb',
                  borderRadius: '8px',
                  boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                }}
                formatter={(value: number) => [formatPosition(value), 'Position']}
                labelFormatter={(label) => new Date(label).toLocaleDateString()}
              />
              <Line
                type="monotone"
                dataKey="position"
                stroke="#f59e0b"
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          )}
        </ResponsiveContainer>
      </div>

      {/* Change Points */}
      {module1.change_points && module1.change_points.length > 0 && (
        <div className="bg-white border rounded-lg p-6 shadow-sm">
          <h3 className="text-xl font-semibold text-gray-900 mb-4 flex items-center">
            <AlertCircle className="w-5 h-5 mr-2 text-orange-600" />
            Significant Traffic Changes
          </h3>
          <div className="space-y-3">
            {module1.change_points.map((changePoint, index) => (
              <div
                key={index}
                className={`p-4 rounded-lg border ${
                  changePoint.direction === 'drop'
                    ? 'bg-red-50 border-red-200'
                    : 'bg-green-50 border-green-200'
                }`}
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    {changePoint.direction === 'drop' ? (
                      <TrendingDown className="w-5 h-5 text-red-600" />
                    ) : (
                      <TrendingUp className="w-5 h-5 text-green-600" />
                    )}
                    <div>
                      <span className="font-semibold text-gray-900">
                        {new Date(changePoint.date).toLocaleDateString()}
                      </span>
                      <span className="text-gray-600 ml-2">
                        {changePoint.direction === 'drop' ? 'Traffic Drop' : 'Traffic Spike'}
                      </span>
                    </div>
                  </div>
                  <span
                    className={`font-semibold ${
                      changePoint.direction === 'drop' ? 'text-red-600' : 'text-green-600'
                    }`}
                  >
                    {changePoint.magnitude > 0 ? '+' : ''}
                    {(changePoint.magnitude * 100).toFixed(1)}%
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Anomalies */}
      {module1.anomalies && module1.anomalies.length > 0 && (
        <div className="bg-white border rounded-lg p-6 shadow-sm">
          <h3 className="text-xl font-semibold text-gray-900 mb-4 flex items-center">
            <AlertCircle className="w-5 h-5 mr-2 text-purple-600" />
            Detected Anomalies
          </h3>
          <div className="space-y-3">
            {module1.anomalies.map((anomaly, index) => (
              <div
                key={index}
                className="p-4 rounded-lg border bg-purple-50 border-purple-200"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    <Activity className="w-5 h-5 text-purple-600" />
                    <div>
                      <span className="font-semibold text-gray-900">
                        {new Date(anomaly.date).toLocaleDateString()}
                      </span>
                      <span className="text-gray-600 ml-2">
                        {anomaly.type === 'discord' ? 'Unusual Pattern' : 'Recurring Pattern'}
                      </span>
                    </div>
                  </div>
                  <span className="font-semibold text-purple-600">
                    {anomaly.magnitude > 0 ? '+' : ''}
                    {(anomaly.magnitude * 100).toFixed(1)}%
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Forecast */}
      <div className="bg-white border rounded-lg p-6 shadow-sm">
        <h3 className="text-xl font-semibold text-gray-900 mb-6 flex items-center">
          <TrendingUp className="w-5 h-5 mr-2 text-blue-600" />
          Traffic Forecast
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {Object.entries(module1.forecast).map(([period, forecast]) => (
            <div key={period} className="bg-gradient-to-br from-blue-50 to-indigo-50 border border-blue-200 rounded-lg p-6">
              <div className="text-sm font-medium text-gray-600 mb-2">
                {period === '30d' ? '30-Day' : period === '60d' ? '60-Day' : '90-Day'} Forecast
              </div>
              <div className="text-3xl font-bold text-gray-900 mb-2">
                {formatNumber(forecast.clicks)}
              </div>
              <div className="text-sm text-gray-600">
                Range: {formatNumber(forecast.ci_low)} - {formatNumber(forecast.ci_high)}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Top Performing Pages */}
      {module1.top_pages && module1.top_pages.length > 0 && (
        <div className="bg-white border rounded-lg p-6 shadow-sm">
          <h3 className="text-xl font-semibold text-gray-900 mb-4">Top Performing Pages</h3>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Page</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Clicks</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Impressions</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">CTR</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Position</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Change</th>
                </tr>
              </thead>
              <tbody>
                {module1.top_pages.slice(0, 10).map((page, index) => (
                  <tr key={index} className="border-b border-gray-100 hover:bg-gray-50">
                    <td className="py-3 px-4 text-sm text-gray-900 max-w-xs truncate">
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
                      {formatPosition(page.position)}
                    </td>
                    <td className={`py-3 px-4 text-sm text-right font-medium ${getChangeColor(page.clicks_change_pct)}`}>
                      {page.clicks_change_pct > 0 ? '+' : ''}
                      {page.clicks_change_pct.toFixed(1)}%
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Query Performance */}
      {module1.query_performance && module1.query_performance.length > 0 && (
        <div className="bg-white border rounded-lg p-6 shadow-sm">
          <h3 className="text-xl font-semibold text-gray-900 mb-4">Top Query Performance</h3>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Query</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Clicks</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Impressions</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">CTR</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Position</th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-gray-700">Change</th>
                </tr>
              </thead>
              <tbody>
                {module1.query_performance.slice(0, 10).map((query, index) => (
                  <tr key={index} className="border-b border-gray-100 hover:bg-gray-50">
                    <td className="py-3 px-4 text-sm text-gray-900 max-w-xs truncate">
                      {query.query}
                    </td>
                    <td className="py-3 px-4 text-sm text-gray-900 text-right font-medium">
                      {formatNumber(query.clicks)}
                    </td>
                    <td className="py-3 px-4 text-sm text-gray-600 text-right">
                      {formatNumber(query.impressions)}
                    </td>
                    <td className="py-3 px-4 text-sm text-gray-600 text-right">
                      {formatPercent(query.ctr)}
                    </td>
                    <td className="py-3 px-4 text-sm text-gray-600 text-right">
                      {formatPosition(query.position)}
                    </td>
                    <td className={`py-3 px-4 text-sm text-right font-medium ${getChangeColor(query.clicks_change_pct)}`}>
                      {query.clicks_change_pct > 0 ? '+' : ''}
                      {query.clicks_change_pct.toFixed(1)}%
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Device and Country Breakdowns */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Device Breakdown */}
        {module1.device_breakdown && module1.device_breakdown.length > 0 && (
          <div className="bg-white border rounded-lg p-6 shadow-sm">
            <h3 className="text-xl font-semibold text-gray-900 mb-4 flex items-center">
              <Monitor className="w-5 h-5 mr-2 text-blue-600" />
              Device Breakdown
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={module1.device_breakdown}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ device, percentage }) => `${device}: ${percentage.toFixed(1)}%`}
                  outerRadius={100}
                  fill="#8884d8"
                  dataKey="clicks"
                >
                  {module1.device_breakdown.map((entry, index) => (
                    <Cell
                      key={`cell-${index}`}
                      fill={DEVICE_COLORS[entry.device.toLowerCase() as keyof typeof DEVICE_COLORS] || '#6b7280'}
                    />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'white',
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px',
                    boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                  }}
                  formatter={(value: number, name: string, props: any) => [
                    `${formatNumber(value)} clicks (${props.payload.percentage.toFixed(1)}%)`,
                    props.payload.device,
                  ]}
                />
              </PieChart>
            </ResponsiveContainer>
            <div className="mt-4 space-y-2">
              {module1.device_breakdown.map((device, index) => (
                <div key={index} className="flex items-center justify-between text-sm">
                  <div className="flex items-center space-x-2">
                    <div
                      className="w-3 h-3 rounded-full"
                      style={{
                        backgroundColor: DEVICE_COLORS[device.device.toLowerCase() as keyof typeof DEVICE_COLORS] || '#6b7280',
                      }}
                    />
                    <span className="text-gray-700 capitalize">{device.device}</span>
                  </div>
                  <div className="text-right">
                    <div className="font-medium text-gray-900">{formatNumber(device.clicks)}</div>
                    <div className="text-xs text-gray-500">{device.percentage.toFixed(1)}%</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Country Breakdown */}
        {module1.country_breakdown && module1.country_breakdown.length > 0 && (
          <div className="bg-white border rounded-lg p-6 shadow-sm">
            <h3 className="text-xl font-semibold text-gray-900 mb-4 flex items-center">
              <MapPin className="w-5 h-5 mr-2 text-green-600" />
              Country Breakdown
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={module1.country_breakdown.slice(0, 5)}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="country" stroke="#6b7280" fontSize={12} />
                <YAxis stroke="#6b7280" fontSize={12} tickFormatter={formatNumber} />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'white',
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px',
                    boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                  }}
                  formatter={(value: number, name: string, props: any) => [
                    `${formatNumber(value)} clicks (${props.payload.percentage.toFixed(1)}%)`,
                    'Clicks',
                  ]}
                />
                <Bar dataKey="clicks" fill="#10b981" radius={[8, 8, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
            <div className="mt-4 space-y-2">
              {module1.country_breakdown.slice(0, 5).map((country, index) => (
                <div key={index} className="flex items-center justify-between text-sm">
                  <span className="text-gray-700">{country.country}</span>
                  <div className="text-right">
                    <div className="font-medium text-gray-900">{formatNumber(country.clicks)}</div>
                    <div className="text-xs text-gray-500">{country.percentage.toFixed(1)}%</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default Module1TrafficOverview;
