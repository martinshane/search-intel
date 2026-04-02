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
import { TrendingUp, TrendingDown, Minus, AlertCircle, Calendar, Activity, Monitor, Globe, Eye, MousePointerClick, Target, Search } from 'lucide-react';

interface TimeSeriesDataPoint {
  date: string;
  clicks: number;
  impressions: number;
  ctr: number;
  position: number;
  sessions?: number;
  users?: number;
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
  description?: string;
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

const DEVICE_COLORS: { [key: string]: string } = {
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

const directionConfig = {
  strong_growth: {
    label: 'Strong Growth',
    color: 'text-green-600',
    bgColor: 'bg-green-50',
    icon: TrendingUp,
    borderColor: 'border-green-200',
  },
  growth: {
    label: 'Growth',
    color: 'text-green-600',
    bgColor: 'bg-green-50',
    icon: TrendingUp,
    borderColor: 'border-green-200',
  },
  flat: {
    label: 'Stable',
    color: 'text-gray-600',
    bgColor: 'bg-gray-50',
    icon: Minus,
    borderColor: 'border-gray-200',
  },
  decline: {
    label: 'Declining',
    color: 'text-orange-600',
    bgColor: 'bg-orange-50',
    icon: TrendingDown,
    borderColor: 'border-orange-200',
  },
  strong_decline: {
    label: 'Strong Decline',
    color: 'text-red-600',
    bgColor: 'bg-red-50',
    icon: TrendingDown,
    borderColor: 'border-red-200',
  },
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

const formatPercent = (num: number): string => {
  return (num * 100).toFixed(2) + '%';
};

const formatPosition = (num: number): string => {
  return num.toFixed(1);
};

const formatDate = (dateStr: string): string => {
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
};

const Module1TrafficOverview: React.FC<Module1TrafficOverviewProps> = ({ reportId }) => {
  const [module1, setModule1] = useState<Module1Data | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [chartView, setChartView] = useState<'clicks' | 'impressions' | 'ctr' | 'position'>('clicks');
  const [comparisonView, setComparisonView] = useState<'separate' | 'overlay'>('separate');

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
        setError(err instanceof Error ? err.message : 'Failed to load module data');
      } finally {
        setLoading(false);
      }
    };

    if (reportId) {
      fetchData();
    }
  }, [reportId]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-16">
        <div className="flex items-center space-x-3">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
          <span className="text-gray-600">Loading traffic overview...</span>
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
            <h3 className="text-red-800 font-semibold mb-1">Error Loading Data</h3>
            <p className="text-red-700">{error}</p>
          </div>
        </div>
      </div>
    );
  }

  if (!module1) {
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-6 text-center">
        <p className="text-gray-600">No data available</p>
      </div>
    );
  }

  const directionInfo = directionConfig[module1.overall_direction];
  const DirectionIcon = directionInfo.icon;

  const summary = module1.traffic_overview.summary;
  const timeseries = module1.traffic_overview.timeseries;

  // Prepare chart data with both GSC and GA4 metrics
  const chartData = timeseries.map(point => ({
    date: formatDate(point.date),
    fullDate: point.date,
    clicks: point.clicks,
    impressions: point.impressions,
    ctr: point.ctr * 100, // Convert to percentage
    position: point.position,
    sessions: point.sessions || 0,
    users: point.users || 0,
  }));

  // Get last 30 days for comparison
  const last30Days = timeseries.slice(-30);
  const previous30Days = timeseries.slice(-60, -30);

  const avg30DayClicks = last30Days.reduce((sum, d) => sum + d.clicks, 0) / last30Days.length;
  const avgPrevious30DayClicks = previous30Days.length > 0 
    ? previous30Days.reduce((sum, d) => sum + d.clicks, 0) / previous30Days.length 
    : avg30DayClicks;

  const clicksChange30d = avgPrevious30DayClicks > 0 
    ? ((avg30DayClicks - avgPrevious30DayClicks) / avgPrevious30DayClicks) * 100 
    : 0;

  return (
    <div className="space-y-8">
      {/* Header Section */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-start justify-between mb-6">
          <div>
            <h2 className="text-2xl font-bold text-gray-900 mb-2">Traffic Health & Trajectory</h2>
            <p className="text-gray-600">
              Comprehensive overview of your search and site traffic performance over the past 16 months
            </p>
          </div>
          <div className={`flex items-center space-x-2 px-4 py-2 rounded-lg ${directionInfo.bgColor} ${directionInfo.borderColor} border`}>
            <DirectionIcon className={`w-5 h-5 ${directionInfo.color}`} />
            <span className={`font-semibold ${directionInfo.color}`}>{directionInfo.label}</span>
          </div>
        </div>

        {/* Trend Summary */}
        <div className={`border-l-4 ${directionInfo.borderColor} bg-gray-50 p-4 rounded-r-lg`}>
          <p className="text-gray-900 leading-relaxed">
            Your site is currently <span className={`font-semibold ${directionInfo.color}`}>
              {module1.overall_direction.replace('_', ' ')}
            </span> at a rate of{' '}
            <span className="font-semibold">{module1.trend_slope_pct_per_month.toFixed(2)}%</span> per month.
            {module1.change_points.length > 0 && (
              <> We detected <span className="font-semibold">{module1.change_points.length}</span> significant 
              change point{module1.change_points.length > 1 ? 's' : ''} in your traffic pattern.</>
            )}
          </p>
        </div>
      </div>

      {/* Key Metrics Cards - GSC Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <MetricCard
          title="Total Clicks"
          value={formatNumber(summary.total_clicks)}
          change={summary.clicks_change_pct}
          icon={MousePointerClick}
          iconColor="text-blue-600"
          iconBg="bg-blue-50"
        />
        <MetricCard
          title="Total Impressions"
          value={formatNumber(summary.total_impressions)}
          change={summary.impressions_change_pct}
          icon={Eye}
          iconColor="text-purple-600"
          iconBg="bg-purple-50"
        />
        <MetricCard
          title="Average CTR"
          value={formatPercent(summary.avg_ctr)}
          change={summary.ctr_change_pct}
          icon={Target}
          iconColor="text-green-600"
          iconBg="bg-green-50"
          isPercentage
        />
        <MetricCard
          title="Average Position"
          value={formatPosition(summary.avg_position)}
          change={-summary.position_change} // Negative because lower position is better
          icon={Search}
          iconColor="text-orange-600"
          iconBg="bg-orange-50"
          inverse
        />
      </div>

      {/* GA4 Metrics Cards (if available) */}
      {(module1.total_sessions || module1.total_users) && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {module1.total_sessions && (
            <MetricCard
              title="Total Sessions"
              value={formatNumber(module1.total_sessions)}
              icon={Activity}
              iconColor="text-indigo-600"
              iconBg="bg-indigo-50"
            />
          )}
          {module1.total_users && (
            <MetricCard
              title="Total Users"
              value={formatNumber(module1.total_users)}
              icon={Globe}
              iconColor="text-cyan-600"
              iconBg="bg-cyan-50"
            />
          )}
          {module1.total_pageviews && (
            <MetricCard
              title="Total Pageviews"
              value={formatNumber(module1.total_pageviews)}
              icon={Eye}
              iconColor="text-pink-600"
              iconBg="bg-pink-50"
            />
          )}
        </div>
      )}

      {/* Chart View Selector */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-6">
          <h3 className="text-lg font-semibold text-gray-900">Traffic Trends</h3>
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2 bg-gray-100 rounded-lg p-1">
              <button
                onClick={() => setChartView('clicks')}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  chartView === 'clicks'
                    ? 'bg-white text-blue-600 shadow-sm'
                    : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                Clicks
              </button>
              <button
                onClick={() => setChartView('impressions')}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  chartView === 'impressions'
                    ? 'bg-white text-purple-600 shadow-sm'
                    : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                Impressions
              </button>
              <button
                onClick={() => setChartView('ctr')}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  chartView === 'ctr'
                    ? 'bg-white text-green-600 shadow-sm'
                    : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                CTR
              </button>
              <button
                onClick={() => setChartView('position')}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  chartView === 'position'
                    ? 'bg-white text-orange-600 shadow-sm'
                    : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                Position
              </button>
            </div>
            {timeseries.some(d => d.sessions) && (
              <div className="flex items-center space-x-2 bg-gray-100 rounded-lg p-1">
                <button
                  onClick={() => setComparisonView('separate')}
                  className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                    comparisonView === 'separate'
                      ? 'bg-white text-gray-900 shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  Separate
                </button>
                <button
                  onClick={() => setComparisonView('overlay')}
                  className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                    comparisonView === 'overlay'
                      ? 'bg-white text-gray-900 shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  Overlay
                </button>
              </div>
            )}
          </div>
        </div>

        {/* Main Chart */}
        <div className="h-96">
          <ResponsiveContainer width="100%" height="100%">
            {chartView === 'clicks' ? (
              <AreaChart data={chartData}>
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
                  tick={{ fontSize: 12 }}
                />
                <YAxis 
                  stroke="#6b7280"
                  tick={{ fontSize: 12 }}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'white',
                    border: '1px solid #e5e7eb',
                    borderRadius: '0.5rem',
                    boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                  }}
                />
                <Legend />
                <Area
                  type="monotone"
                  dataKey="clicks"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  fill="url(#colorClicks)"
                  name="Clicks"
                />
                {module1.change_points.map((cp, idx) => (
                  <Line
                    key={idx}
                    type="monotone"
                    dataKey={() => null}
                    stroke={cp.direction === 'drop' ? '#ef4444' : '#10b981'}
                    strokeWidth={2}
                    strokeDasharray="5 5"
                    dot={false}
                  />
                ))}
              </AreaChart>
            ) : chartView === 'impressions' ? (
              <AreaChart data={chartData}>
                <defs>
                  <linearGradient id="colorImpressions" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis 
                  dataKey="date" 
                  stroke="#6b7280"
                  tick={{ fontSize: 12 }}
                />
                <YAxis 
                  stroke="#6b7280"
                  tick={{ fontSize: 12 }}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'white',
                    border: '1px solid #e5e7eb',
                    borderRadius: '0.5rem',
                    boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                  }}
                />
                <Legend />
                <Area
                  type="monotone"
                  dataKey="impressions"
                  stroke="#8b5cf6"
                  strokeWidth={2}
                  fill="url(#colorImpressions)"
                  name="Impressions"
                />
              </AreaChart>
            ) : chartView === 'ctr' ? (
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis 
                  dataKey="date" 
                  stroke="#6b7280"
                  tick={{ fontSize: 12 }}
                />
                <YAxis 
                  stroke="#6b7280"
                  tick={{ fontSize: 12 }}
                  tickFormatter={(value) => `${value.toFixed(1)}%`}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'white',
                    border: '1px solid #e5e7eb',
                    borderRadius: '0.5rem',
                    boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                  }}
                  formatter={(value: number) => [`${value.toFixed(2)}%`, 'CTR']}
                />
                <Legend />
                <Line
                  type="monotone"
                  dataKey="ctr"
                  stroke="#10b981"
                  strokeWidth={2}
                  dot={false}
                  name="CTR (%)"
                />
              </LineChart>
            ) : (
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis 
                  dataKey="date" 
                  stroke="#6b7280"
                  tick={{ fontSize: 12 }}
                />
                <YAxis 
                  stroke="#6b7280"
                  tick={{ fontSize: 12 }}
                  reversed
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'white',
                    border: '1px solid #e5e7eb',
                    borderRadius: '0.5rem',
                    boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                  }}
                  formatter={(value: number) => [value.toFixed(1), 'Position']}
                />
                <Legend />
                <Line
                  type="monotone"
                  dataKey="position"
                  stroke="#f59e0b"
                  strokeWidth={2}
                  dot={false}
                  name="Average Position"
                />
              </LineChart>
            )}
          </ResponsiveContainer>
        </div>
      </div>

      {/* GSC vs GA4 Comparison */}
      {timeseries.some(d => d.sessions) && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-6">Search Console vs Analytics Comparison</h3>
          
          {comparisonView === 'separate' ? (
            <div className="space-y-6">
              {/* GSC Clicks vs GA4 Sessions */}
              <div>
                <h4 className="text-sm font-medium text-gray-700 mb-3">Clicks (GSC) vs Sessions (GA4)</h4>
                <div className="h-64">
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                      <XAxis 
                        dataKey="date" 
                        stroke="#6b7280"
                        tick={{ fontSize: 12 }}
                      />
                      <YAxis 
                        yAxisId="left"
                        stroke="#3b82f6"
                        tick={{ fontSize: 12 }}
                      />
                      <YAxis 
                        yAxisId="right"
                        orientation="right"
                        stroke="#10b981"
                        tick={{ fontSize: 12 }}
                      />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: 'white',
                          border: '1px solid #e5e7eb',
                          borderRadius: '0.5rem',
                          boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                        }}
                      />
                      <Legend />
                      <Area
                        yAxisId="left"
                        type="monotone"
                        dataKey="clicks"
                        stroke="#3b82f6"
                        fill="#3b82f6"
                        fillOpacity={0.2}
                        name="Clicks (GSC)"
                      />
                      <Line
                        yAxisId="right"
                        type="monotone"
                        dataKey="sessions"
                        stroke="#10b981"
                        strokeWidth={2}
                        dot={false}
                        name="Sessions (GA4)"
                      />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* Users Trend */}
              {timeseries.some(d => d.users) && (
                <div>
                  <h4 className="text-sm font-medium text-gray-700 mb-3">Users (GA4)</h4>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={chartData}>
                        <defs>
                          <linearGradient id="colorUsers" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#06b6d4" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="#06b6d4" stopOpacity={0} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                        <XAxis 
                          dataKey="date" 
                          stroke="#6b7280"
                          tick={{ fontSize: 12 }}
                        />
                        <YAxis 
                          stroke="#6b7280"
                          tick={{ fontSize: 12 }}
                        />
                        <Tooltip
                          contentStyle={{
                            backgroundColor: 'white',
                            border: '1px solid #e5e7eb',
                            borderRadius: '0.5rem',
                            boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                          }}
                        />
                        <Legend />
                        <Area
                          type="monotone"
                          dataKey="users"
                          stroke="#06b6d4"
                          strokeWidth={2}
                          fill="url(#colorUsers)"
                          name="Users"
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              )}
            </div>
          ) : (
            <div className="h-96">
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis 
                    dataKey="date" 
                    stroke="#6b7280"
                    tick={{ fontSize: 12 }}
                  />
                  <YAxis 
                    yAxisId="left"
                    stroke="#6b7280"
                    tick={{ fontSize: 12 }}
                  />
                  <YAxis 
                    yAxisId="right"
                    orientation="right"
                    stroke="#6b7280"
                    tick={{ fontSize: 12 }}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: 'white',
                      border: '1px solid #e5e7eb',
                      borderRadius: '0.5rem',
                      boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                    }}
                  />
                  <Legend />
                  <Area
                    yAxisId="left"
                    type="monotone"
                    dataKey="clicks"
                    stroke="#3b82f6"
                    fill="#3b82f6"
                    fillOpacity={0.2}
                    name="Clicks (GSC)"
                  />
                  <Line
                    yAxisId="right"
                    type="monotone"
                    dataKey="sessions"
                    stroke="#10b981"
                    strokeWidth={2}
                    dot={false}
                    name="Sessions (GA4)"
                  />
                  <Line
                    yAxisId="right"
                    type="monotone"
                    dataKey="users"
                    stroke="#06b6d4"
                    strokeWidth={2}
                    dot={false}
                    strokeDasharray="5 5"
                    name="Users (GA4)"
                  />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>
      )}

      {/* Traffic Sources and Device Breakdown */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Traffic Sources */}
        {module1.traffic_sources && module1.traffic_sources.length > 0 && (
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Sources (GA4)</h3>
            <div className="space-y-4">
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={module1.traffic_sources}
                      cx="50%"
                      cy="50%"
                      labelLine={false}
                      label={({ source, percentage }) => `${source}: ${percentage.toFixed(1)}%`}
                      outerRadius={80}
                      fill="#8884d8"
                      dataKey="sessions"
                    >
                      {module1.traffic_sources.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={SOURCE_COLORS[index % SOURCE_COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'white',
                        border: '1px solid #e5e7eb',
                        borderRadius: '0.5rem',
                        boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                      }}
                    />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="space-y-2">
                {module1.traffic_sources.map((source, index) => (
                  <div key={source.source} className="flex items-center justify-between">
                    <div className="flex items-center space-x-2">
                      <div 
                        className="w-3 h-3 rounded-full"
                        style={{ backgroundColor: SOURCE_COLORS[index % SOURCE_COLORS.length] }}
                      />
                      <span className="text-sm text-gray-700 font-medium">{source.source}</span>
                    </div>
                    <div className="text-right">
                      <div className="text-sm font-semibold text-gray-900">
                        {formatNumber(source.sessions)}
                      </div>
                      <div className="text-xs text-gray-500">
                        {source.percentage.toFixed(1)}%
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* Device Breakdown */}
        {module1.device_breakdown && module1.device_breakdown.length > 0 && (
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Device Breakdown (GA4)</h3>
            <div className="space-y-4">
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={module1.device_breakdown} layout="vertical">
                    <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                    <XAxis type="number" stroke="#6b7280" tick={{ fontSize: 12 }} />
                    <YAxis type="category" dataKey="device" stroke="#6b7280" tick={{ fontSize: 12 }} />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'white',
                        border: '1px solid #e5e7eb',
                        borderRadius: '0.5rem',
                        boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                      }}
                    />
                    <Bar dataKey="sessions" fill="#8884d8">
                      {module1.device_breakdown.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={DEVICE_COLORS[entry.device] || '#94a3b8'} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
              <div className="space-y-2">
                {module1.device_breakdown.map((device) => (
                  <div key={device.device} className="flex items-center justify-between">
                    <div className="flex items-center space-x-2">
                      <Monitor className={`w-4 h-4 ${
                        device.device === 'desktop' ? 'text-blue-600' :
                        device.device === 'mobile' ? 'text-green-600' :
                        'text-orange-600'
                      }`} />
                      <span className="text-sm text-gray-700 font-medium capitalize">{device.device}</span>
                    </div>
                    <div className="text-right">
                      <div className="text-sm font-semibold text-gray-900">
                        {formatNumber(device.sessions)}
                      </div>
                      <div className="text-xs text-gray-500">
                        {device.percentage.toFixed(1)}%
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Seasonality & Patterns */}
      {module1.seasonality && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Seasonality & Patterns</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
              <div className="flex items-center space-x-2 mb-2">
                <Calendar className="w-5 h-5 text-blue-600" />
                <h4 className="font-semibold text-blue-900">Weekly Pattern</h4>
              </div>
              <p className="text-sm text-blue-800">
                <span className="font-semibold">Best day:</span> {module1.seasonality.best_day}
              </p>
              <p className="text-sm text-blue-800">
                <span className="font-semibold">Worst day:</span> {module1.seasonality.worst_day}
              </p>
            </div>
            
            {module1.seasonality.monthly_cycle && (
              <div className="bg-purple-50 border border-purple-200 rounded-lg p-4">
                <div className="flex items-center space-x-2 mb-2">
                  <Activity className="w-5 h-5 text-purple-600" />
                  <h4 className="font-semibold text-purple-900">Monthly Cycle</h4>
                </div>
                <p className="text-sm text-purple-800">{module1.seasonality.cycle_description}</p>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Change Points */}
      {module1.change_points.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Significant Change Points</h3>
          <div className="space-y-3">
            {module1.change_points.map((cp, index) => (
              <div
                key={index}
                className={`flex items-start space-x-3 p-4 rounded-lg border ${
                  cp.direction === 'drop'
                    ? 'bg-red-50 border-red-200'
                    : 'bg-green-50 border-green-200'
                }`}
              >
                {cp.direction === 'drop' ? (
                  <TrendingDown className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
                ) : (
                  <TrendingUp className="w-5 h-5 text-green-600 flex-shrink-0 mt-0.5" />
                )}
                <div className="flex-1">
                  <div className="flex items-center justify-between mb-1">
                    <span className={`font-semibold ${
                      cp.direction === 'drop' ? 'text-red-900' : 'text-green-900'
                    }`}>
                      {cp.direction === 'drop' ? 'Traffic Drop' : 'Traffic Spike'}
                    </span>
                    <span className={`text-sm ${
                      cp.direction === 'drop' ? 'text-red-700' : 'text-green-700'
                    }`}>
                      {new Date(cp.date).toLocaleDateString('en-US', { 
                        year: 'numeric', 
                        month: 'long', 
                        day: 'numeric' 
                      })}
                    </span>
                  </div>
                  <p className={`text-sm ${
                    cp.direction === 'drop' ? 'text-red-800' : 'text-green-800'
                  }`}>
                    Magnitude: {(cp.magnitude * 100).toFixed(1)}% change detected
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Anomalies */}
      {module1.anomalies.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Anomalies</h3>
          <div className="space-y-3">
            {module1.anomalies.map((anomaly, index) => (
              <div
                key={index}
                className="flex items-start space-x-3 p-4 bg-yellow-50 border border-yellow-200 rounded-lg"
              >
                <AlertCircle className="w-5 h-5 text-yellow-600 flex-shrink-0 mt-0.5" />
                <div className="flex-1">
                  <div className="flex items-center justify-between mb-1">
                    <span className="font-semibold text-yellow-900">
                      {anomaly.type === 'discord' ? 'Unusual Pattern' : 'Recurring Pattern'}
                    </span>
                    <span className="text-sm text-yellow-700">
                      {new Date(anomaly.date).toLocaleDateString('en-US', { 
                        year: 'numeric', 
                        month: 'long', 
                        day: 'numeric' 
                      })}
                    </span>
                  </div>
                  <p className="text-sm text-yellow-800">
                    {anomaly.description || `Anomaly magnitude: ${(anomaly.magnitude * 100).toFixed(1)}%`}
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Forecast */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Traffic Forecast</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <ForecastCard
            period="30 Days"
            forecast={module1.forecast['30d']}
          />
          <ForecastCard
            period="60 Days"
            forecast={module1.forecast['60d']}
          />
          <ForecastCard
            period="90 Days"
            forecast={module1.forecast['90d']}
          />
        </div>
        <div className="mt-4 p-4 bg-gray-50 rounded-lg">
          <p className="text-sm text-gray-600">
            <span className="font-semibold">Note:</span> Forecasts are based on historical patterns and trends. 
            Actual results may vary due to seasonality, algorithm updates, competitive changes, and other factors.
          </p>
        </div>
      </div>
    </div>
  );
};

interface MetricCardProps {
  title: string;
  value: string;
  change?: number;
  icon: React.ComponentType<{ className?: string }>;
  iconColor: string;
  iconBg: string;
  isPercentage?: boolean;
  inverse?: boolean;
}

const MetricCard: React.FC<MetricCardProps> = ({
  title,
  value,
  change,
  icon: Icon,
  iconColor,
  iconBg,
  isPercentage = false,
  inverse = false,
}) => {
  const hasChange = change !== undefined && !isNaN(change);
  const isPositive = inverse ? change! < 0 : change! > 0;
  const isNegative = inverse ? change! > 0 : change! < 0;

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-4">
        <div className={`p-3 rounded-lg ${iconBg}`}>
          <Icon className={`w-6 h-6 ${iconColor}`} />
        </div>
        {hasChange && (
          <div className={`flex items-center space-x-1 ${
            isPositive ? 'text-green-600' : isNegative ? 'text-red-600' : 'text-gray-600'
          }`}>
            {isPositive ? (
              <TrendingUp className="w-4 h-4" />
            ) : isNegative ? (
              <TrendingDown className="w-4 h-4" />
            ) : (
              <Minus className="w-4 h-4" />
            )}
            <span className="text-sm font-semibold">
              {Math.abs(change!).toFixed(1)}%
            </span>
          </div>
        )}
      </div>
      <div>
        <p className="text-sm text-gray-600 mb-1">{title}</p>
        <p className="text-2xl font-bold text-gray-900">{value}</p>
      </div>
    </div>
  );
};

interface ForecastCardProps {
  period: string;
  forecast: Forecast;
}

const ForecastCard: React.FC<ForecastCardProps> = ({ period, forecast }) => {
  return (
    <div className="bg-gradient-to-br from-blue-50 to-indigo-50 border border-blue-200 rounded-lg p-6">
      <h4 className="text-sm font-semibold text-blue-900 mb-3">{period}</h4>
      <div className="space-y-2">
        <div>
          <p className="text-xs text-blue-700 mb-1">Projected Clicks</p>
          <p className="text-2xl font-bold text-blue-900">{formatNumber(forecast.clicks)}</p>
        </div>
        <div className="pt-2 border-t border-blue-200">
          <p className="text-xs text-blue-700 mb-1">Confidence Range</p>
          <p className="text-sm font-semibold text-blue-800">
            {formatNumber(forecast.ci_low)} - {formatNumber(forecast.ci_high)}
          </p>
        </div>
      </div>
    </div>
  );
};

export default Module1TrafficOverview;
