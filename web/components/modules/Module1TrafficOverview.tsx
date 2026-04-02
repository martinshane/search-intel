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
} from 'recharts';
import { TrendingUp, TrendingDown, Minus, AlertCircle, Calendar, Activity, Monitor, Globe } from 'lucide-react';

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

interface TrafficSource {
  source: string;
  sessions: number;
  percentage: number;
}

interface DeviceBreakdown {
  device: string;
  sessions: number;
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
  traffic_sources?: TrafficSource[];
  device_breakdown?: DeviceBreakdown[];
  total_sessions?: number;
  total_users?: number;
  total_pageviews?: number;
}

interface Module1TrafficOverviewProps {
  reportId: string;
}

const DEVICE_COLORS = {
  desktop: '#3b82f6',
  mobile: '#10b981',
  tablet: '#f59e0b',
};

const SOURCE_COLORS = [
  '#3b82f6',
  '#10b981',
  '#f59e0b',
  '#ef4444',
  '#8b5cf6',
  '#ec4899',
  '#06b6d4',
  '#84cc16',
];

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
        setError(err instanceof Error ? err.message : 'An error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
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
        return 'Flat';
      case 'decline':
        return 'Decline';
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

  const getChartData = () => {
    if (!module1?.traffic_overview?.timeseries) return [];
    
    return module1.traffic_overview.timeseries.map(point => ({
      ...point,
      date: new Date(point.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
    }));
  };

  const getChartDataKey = (): string => {
    return chartView;
  };

  const getChartLabel = (): string => {
    switch (chartView) {
      case 'clicks':
        return 'Clicks';
      case 'impressions':
        return 'Impressions';
      case 'ctr':
        return 'CTR';
      case 'position':
        return 'Avg Position';
      default:
        return '';
    }
  };

  const formatChartValue = (value: number): string => {
    switch (chartView) {
      case 'clicks':
      case 'impressions':
        return formatNumber(value);
      case 'ctr':
        return formatPercent(value);
      case 'position':
        return formatPosition(value);
      default:
        return value.toString();
    }
  };

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow-md p-8">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-200 rounded w-1/3 mb-6"></div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
            {[1, 2, 3].map(i => (
              <div key={i} className="h-32 bg-gray-200 rounded"></div>
            ))}
          </div>
          <div className="h-96 bg-gray-200 rounded mb-8"></div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="h-64 bg-gray-200 rounded"></div>
            <div className="h-64 bg-gray-200 rounded"></div>
          </div>
        </div>
      </div>
    );
  }

  if (error || !module1) {
    return (
      <div className="bg-white rounded-lg shadow-md p-8">
        <div className="flex items-center gap-3 text-red-600">
          <AlertCircle className="w-6 h-6" />
          <div>
            <h3 className="font-semibold text-lg">Error Loading Data</h3>
            <p className="text-sm text-gray-600">{error || 'Module data not available'}</p>
          </div>
        </div>
      </div>
    );
  }

  const chartData = getChartData();
  const summary = module1.traffic_overview?.summary;

  return (
    <div className="bg-white rounded-lg shadow-md p-8">
      {/* Header */}
      <div className="mb-8">
        <h2 className="text-3xl font-bold text-gray-900 mb-2">Traffic Overview & Health</h2>
        <p className="text-gray-600">
          Comprehensive analysis of your search traffic trends, trajectory, and performance metrics
        </p>
      </div>

      {/* Overall Direction Card */}
      <div className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-lg p-6 mb-8 border border-blue-200">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            {getTrendIcon(module1.overall_direction)}
            <div>
              <h3 className="text-lg font-semibold text-gray-900">Overall Trajectory</h3>
              <p className={`text-2xl font-bold ${getTrendColor(module1.overall_direction)}`}>
                {getTrendLabel(module1.overall_direction)}
              </p>
            </div>
          </div>
          <div className="text-right">
            <p className="text-sm text-gray-600">Monthly Trend</p>
            <p className={`text-2xl font-bold ${getChangeColor(module1.trend_slope_pct_per_month)}`}>
              {module1.trend_slope_pct_per_month > 0 ? '+' : ''}
              {module1.trend_slope_pct_per_month.toFixed(1)}%
            </p>
          </div>
        </div>
      </div>

      {/* Summary Metrics Cards */}
      {summary && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
          {/* Total Sessions */}
          {module1.total_sessions !== undefined && (
            <div className="bg-white border border-gray-200 rounded-lg p-6 hover:shadow-lg transition-shadow">
              <div className="flex items-center gap-3 mb-2">
                <Activity className="w-5 h-5 text-blue-600" />
                <h3 className="text-sm font-medium text-gray-600">Total Sessions</h3>
              </div>
              <p className="text-3xl font-bold text-gray-900">{formatNumber(module1.total_sessions)}</p>
            </div>
          )}

          {/* Total Users */}
          {module1.total_users !== undefined && (
            <div className="bg-white border border-gray-200 rounded-lg p-6 hover:shadow-lg transition-shadow">
              <div className="flex items-center gap-3 mb-2">
                <Globe className="w-5 h-5 text-green-600" />
                <h3 className="text-sm font-medium text-gray-600">Total Users</h3>
              </div>
              <p className="text-3xl font-bold text-gray-900">{formatNumber(module1.total_users)}</p>
            </div>
          )}

          {/* Total Pageviews */}
          {module1.total_pageviews !== undefined && (
            <div className="bg-white border border-gray-200 rounded-lg p-6 hover:shadow-lg transition-shadow">
              <div className="flex items-center gap-3 mb-2">
                <Monitor className="w-5 h-5 text-purple-600" />
                <h3 className="text-sm font-medium text-gray-600">Total Pageviews</h3>
              </div>
              <p className="text-3xl font-bold text-gray-900">{formatNumber(module1.total_pageviews)}</p>
            </div>
          )}

          {/* Total Clicks */}
          <div className="bg-white border border-gray-200 rounded-lg p-6 hover:shadow-lg transition-shadow">
            <div className="flex items-center gap-3 mb-2">
              <Activity className="w-5 h-5 text-blue-600" />
              <h3 className="text-sm font-medium text-gray-600">Total Clicks</h3>
            </div>
            <p className="text-3xl font-bold text-gray-900">{formatNumber(summary.total_clicks)}</p>
            {summary.clicks_change_pct !== undefined && (
              <p className={`text-sm ${getChangeColor(summary.clicks_change_pct)} mt-1`}>
                {summary.clicks_change_pct > 0 ? '+' : ''}
                {summary.clicks_change_pct.toFixed(1)}% vs prev period
              </p>
            )}
          </div>

          {/* Total Impressions */}
          <div className="bg-white border border-gray-200 rounded-lg p-6 hover:shadow-lg transition-shadow">
            <div className="flex items-center gap-3 mb-2">
              <Globe className="w-5 h-5 text-green-600" />
              <h3 className="text-sm font-medium text-gray-600">Total Impressions</h3>
            </div>
            <p className="text-3xl font-bold text-gray-900">{formatNumber(summary.total_impressions)}</p>
            {summary.impressions_change_pct !== undefined && (
              <p className={`text-sm ${getChangeColor(summary.impressions_change_pct)} mt-1`}>
                {summary.impressions_change_pct > 0 ? '+' : ''}
                {summary.impressions_change_pct.toFixed(1)}% vs prev period
              </p>
            )}
          </div>

          {/* Average CTR */}
          <div className="bg-white border border-gray-200 rounded-lg p-6 hover:shadow-lg transition-shadow">
            <div className="flex items-center gap-3 mb-2">
              <TrendingUp className="w-5 h-5 text-purple-600" />
              <h3 className="text-sm font-medium text-gray-600">Average CTR</h3>
            </div>
            <p className="text-3xl font-bold text-gray-900">{formatPercent(summary.avg_ctr)}</p>
            {summary.ctr_change_pct !== undefined && (
              <p className={`text-sm ${getChangeColor(summary.ctr_change_pct)} mt-1`}>
                {summary.ctr_change_pct > 0 ? '+' : ''}
                {summary.ctr_change_pct.toFixed(1)}% vs prev period
              </p>
            )}
          </div>

          {/* Average Position */}
          <div className="bg-white border border-gray-200 rounded-lg p-6 hover:shadow-lg transition-shadow">
            <div className="flex items-center gap-3 mb-2">
              <Calendar className="w-5 h-5 text-orange-600" />
              <h3 className="text-sm font-medium text-gray-600">Average Position</h3>
            </div>
            <p className="text-3xl font-bold text-gray-900">{formatPosition(summary.avg_position)}</p>
            {summary.position_change !== undefined && (
              <p className={`text-sm ${getChangeColor(-summary.position_change)} mt-1`}>
                {summary.position_change > 0 ? '+' : ''}
                {summary.position_change.toFixed(1)} vs prev period
              </p>
            )}
          </div>
        </div>
      )}

      {/* Traffic Trend Chart */}
      {chartData.length > 0 && (
        <div className="mb-8">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-xl font-semibold text-gray-900">Traffic Trends Over Time</h3>
            <div className="flex gap-2">
              <button
                onClick={() => setChartView('clicks')}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  chartView === 'clicks'
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                Clicks
              </button>
              <button
                onClick={() => setChartView('impressions')}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  chartView === 'impressions'
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                Impressions
              </button>
              <button
                onClick={() => setChartView('ctr')}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  chartView === 'ctr'
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                CTR
              </button>
              <button
                onClick={() => setChartView('position')}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  chartView === 'position'
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                Position
              </button>
            </div>
          </div>
          <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
            <ResponsiveContainer width="100%" height={400}>
              <AreaChart data={chartData}>
                <defs>
                  <linearGradient id="colorMetric" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis
                  dataKey="date"
                  stroke="#6b7280"
                  tick={{ fontSize: 12 }}
                  tickLine={false}
                />
                <YAxis
                  stroke="#6b7280"
                  tick={{ fontSize: 12 }}
                  tickLine={false}
                  tickFormatter={formatChartValue}
                  reversed={chartView === 'position'}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#fff',
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px',
                    padding: '12px',
                  }}
                  formatter={(value: number) => [formatChartValue(value), getChartLabel()]}
                  labelStyle={{ fontWeight: 'bold', marginBottom: '8px' }}
                />
                <Area
                  type="monotone"
                  dataKey={getChartDataKey()}
                  stroke="#3b82f6"
                  strokeWidth={2}
                  fill="url(#colorMetric)"
                  animationDuration={1000}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Device Breakdown & Traffic Sources */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
        {/* Device Breakdown */}
        {module1.device_breakdown && module1.device_breakdown.length > 0 && (
          <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
            <h3 className="text-xl font-semibold text-gray-900 mb-4 flex items-center gap-2">
              <Monitor className="w-5 h-5 text-blue-600" />
              Device Breakdown
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={module1.device_breakdown}
                  dataKey="sessions"
                  nameKey="device"
                  cx="50%"
                  cy="50%"
                  outerRadius={100}
                  label={(entry) => `${entry.device}: ${entry.percentage.toFixed(1)}%`}
                  animationDuration={800}
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
                    backgroundColor: '#fff',
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px',
                    padding: '12px',
                  }}
                  formatter={(value: number, name: string, entry: any) => [
                    `${formatNumber(value)} (${entry.payload.percentage.toFixed(1)}%)`,
                    name,
                  ]}
                />
              </PieChart>
            </ResponsiveContainer>
            <div className="mt-4 space-y-2">
              {module1.device_breakdown.map((device, index) => (
                <div key={index} className="flex items-center justify-between text-sm">
                  <div className="flex items-center gap-2">
                    <div
                      className="w-3 h-3 rounded-full"
                      style={{
                        backgroundColor:
                          DEVICE_COLORS[device.device.toLowerCase() as keyof typeof DEVICE_COLORS] || '#6b7280',
                      }}
                    ></div>
                    <span className="font-medium text-gray-700 capitalize">{device.device}</span>
                  </div>
                  <span className="text-gray-900 font-semibold">{formatNumber(device.sessions)}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Traffic Sources */}
        {module1.traffic_sources && module1.traffic_sources.length > 0 && (
          <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
            <h3 className="text-xl font-semibold text-gray-900 mb-4 flex items-center gap-2">
              <Globe className="w-5 h-5 text-green-600" />
              Top Traffic Sources
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={module1.traffic_sources} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis
                  type="number"
                  stroke="#6b7280"
                  tick={{ fontSize: 12 }}
                  tickLine={false}
                  tickFormatter={formatNumber}
                />
                <YAxis
                  type="category"
                  dataKey="source"
                  stroke="#6b7280"
                  tick={{ fontSize: 12 }}
                  tickLine={false}
                  width={100}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#fff',
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px',
                    padding: '12px',
                  }}
                  formatter={(value: number, name: string, entry: any) => [
                    `${formatNumber(value)} (${entry.payload.percentage.toFixed(1)}%)`,
                    'Sessions',
                  ]}
                />
                <Bar dataKey="sessions" radius={[0, 4, 4, 0]} animationDuration={800}>
                  {module1.traffic_sources.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={SOURCE_COLORS[index % SOURCE_COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>

      {/* Change Points */}
      {module1.change_points && module1.change_points.length > 0 && (
        <div className="bg-yellow-50 rounded-lg p-6 mb-8 border border-yellow-200">
          <h3 className="text-xl font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <AlertCircle className="w-5 h-5 text-yellow-600" />
            Significant Change Points
          </h3>
          <div className="space-y-3">
            {module1.change_points.map((point, index) => (
              <div
                key={index}
                className="flex items-center justify-between bg-white rounded-md p-4 border border-yellow-200"
              >
                <div className="flex items-center gap-3">
                  {point.direction === 'spike' ? (
                    <TrendingUp className="w-5 h-5 text-green-600" />
                  ) : (
                    <TrendingDown className="w-5 h-5 text-red-600" />
                  )}
                  <div>
                    <p className="font-medium text-gray-900">
                      {new Date(point.date).toLocaleDateString('en-US', {
                        year: 'numeric',
                        month: 'long',
                        day: 'numeric',
                      })}
                    </p>
                    <p className="text-sm text-gray-600">
                      {point.direction === 'spike' ? 'Traffic Spike' : 'Traffic Drop'}
                    </p>
                  </div>
                </div>
                <div className="text-right">
                  <p
                    className={`text-lg font-bold ${
                      point.magnitude > 0 ? 'text-green-600' : 'text-red-600'
                    }`}
                  >
                    {point.magnitude > 0 ? '+' : ''}
                    {(point.magnitude * 100).toFixed(1)}%
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Seasonality */}
      {module1.seasonality && (
        <div className="bg-blue-50 rounded-lg p-6 mb-8 border border-blue-200">
          <h3 className="text-xl font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <Calendar className="w-5 h-5 text-blue-600" />
            Seasonality Patterns
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="bg-white rounded-md p-4 border border-blue-200">
              <p className="text-sm text-gray-600 mb-1">Best Day</p>
              <p className="text-xl font-bold text-green-600">{module1.seasonality.best_day}</p>
            </div>
            <div className="bg-white rounded-md p-4 border border-blue-200">
              <p className="text-sm text-gray-600 mb-1">Worst Day</p>
              <p className="text-xl font-bold text-red-600">{module1.seasonality.worst_day}</p>
            </div>
          </div>
          {module1.seasonality.monthly_cycle && (
            <div className="mt-4 bg-white rounded-md p-4 border border-blue-200">
              <p className="text-sm text-gray-600 mb-1">Monthly Pattern</p>
              <p className="text-gray-900">{module1.seasonality.cycle_description}</p>
            </div>
          )}
        </div>
      )}

      {/* Forecast */}
      {module1.forecast && (
        <div className="bg-gradient-to-r from-purple-50 to-pink-50 rounded-lg p-6 border border-purple-200">
          <h3 className="text-xl font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <TrendingUp className="w-5 h-5 text-purple-600" />
            Traffic Forecast
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {['30d', '60d', '90d'].map((period) => {
              const forecast = module1.forecast[period as keyof typeof module1.forecast];
              return (
                <div key={period} className="bg-white rounded-md p-4 border border-purple-200">
                  <p className="text-sm text-gray-600 mb-2">{period.replace('d', ' Days')}</p>
                  <p className="text-2xl font-bold text-gray-900 mb-1">
                    {formatNumber(forecast.clicks)}
                  </p>
                  <p className="text-xs text-gray-600">
                    Range: {formatNumber(forecast.ci_low)} - {formatNumber(forecast.ci_high)}
                  </p>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Anomalies */}
      {module1.anomalies && module1.anomalies.length > 0 && (
        <div className="bg-red-50 rounded-lg p-6 mt-8 border border-red-200">
          <h3 className="text-xl font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <AlertCircle className="w-5 h-5 text-red-600" />
            Detected Anomalies
          </h3>
          <div className="space-y-2">
            {module1.anomalies.slice(0, 5).map((anomaly, index) => (
              <div
                key={index}
                className="flex items-center justify-between bg-white rounded-md p-3 border border-red-200"
              >
                <div>
                  <p className="font-medium text-gray-900">
                    {new Date(anomaly.date).toLocaleDateString('en-US', {
                      year: 'numeric',
                      month: 'short',
                      day: 'numeric',
                    })}
                  </p>
                  <p className="text-sm text-gray-600 capitalize">{anomaly.type}</p>
                </div>
                <p
                  className={`text-lg font-bold ${
                    anomaly.magnitude > 0 ? 'text-green-600' : 'text-red-600'
                  }`}
                >
                  {anomaly.magnitude > 0 ? '+' : ''}
                  {(anomaly.magnitude * 100).toFixed(1)}%
                </p>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default Module1TrafficOverview;
