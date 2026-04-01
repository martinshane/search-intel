import React, { useMemo } from 'react';
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
} from 'recharts';
import {
  TrendingUp,
  TrendingDown,
  Minus,
  Monitor,
  Smartphone,
  Tablet,
  ArrowUpRight,
  ArrowDownRight,
} from 'lucide-react';

interface Module1PerformanceProps {
  data: {
    module1_performance?: {
      traffic_trend?: {
        daily_data: Array<{
          date: string;
          clicks: number;
          impressions: number;
          ctr: number;
          position: number;
        }>;
        summary: {
          total_clicks_90d: number;
          total_impressions_90d: number;
          avg_ctr_90d: number;
          avg_position_90d: number;
          trend_direction: string;
          trend_slope_pct: number;
          clicks_change_pct: number;
          impressions_change_pct: number;
        };
      };
      top_pages?: Array<{
        url: string;
        clicks: number;
        impressions: number;
        ctr: number;
        position: number;
        clicks_change_pct?: number;
      }>;
      device_breakdown?: {
        desktop: {
          clicks: number;
          impressions: number;
          ctr: number;
          position: number;
        };
        mobile: {
          clicks: number;
          impressions: number;
          ctr: number;
          position: number;
        };
        tablet: {
          clicks: number;
          impressions: number;
          ctr: number;
          position: number;
        };
      };
    };
  };
}

const DEVICE_COLORS = {
  desktop: '#3b82f6',
  mobile: '#10b981',
  tablet: '#f59e0b',
};

const formatNumber = (num: number): string => {
  if (num >= 1000000) {
    return `${(num / 1000000).toFixed(1)}M`;
  }
  if (num >= 1000) {
    return `${(num / 1000).toFixed(1)}K`;
  }
  return num.toLocaleString();
};

const formatPercent = (num: number): string => {
  return `${(num * 100).toFixed(2)}%`;
};

const formatDate = (dateStr: string): string => {
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
};

const TrendIndicator: React.FC<{ value: number; label: string }> = ({ value, label }) => {
  const isPositive = value > 0;
  const isNeutral = Math.abs(value) < 0.1;

  return (
    <div className="flex items-center gap-2">
      <div
        className={`flex items-center gap-1 px-2 py-1 rounded text-sm font-medium ${
          isNeutral
            ? 'bg-gray-100 text-gray-700'
            : isPositive
            ? 'bg-green-100 text-green-700'
            : 'bg-red-100 text-red-700'
        }`}
      >
        {isNeutral ? (
          <Minus className="w-4 h-4" />
        ) : isPositive ? (
          <TrendingUp className="w-4 h-4" />
        ) : (
          <TrendingDown className="w-4 h-4" />
        )}
        <span>{Math.abs(value).toFixed(1)}%</span>
      </div>
      <span className="text-sm text-gray-600">{label}</span>
    </div>
  );
};

const Module1Performance: React.FC<Module1PerformanceProps> = ({ data }) => {
  const module1 = data.module1_performance;

  const trafficChartData = useMemo(() => {
    if (!module1?.traffic_trend?.daily_data) return [];
    
    // Aggregate to weekly for cleaner visualization
    const dailyData = module1.traffic_trend.daily_data;
    const weeklyData: { [key: string]: { clicks: number; impressions: number; count: number } } = {};
    
    dailyData.forEach((day) => {
      const date = new Date(day.date);
      const weekStart = new Date(date);
      weekStart.setDate(date.getDate() - date.getDay());
      const weekKey = weekStart.toISOString().split('T')[0];
      
      if (!weeklyData[weekKey]) {
        weeklyData[weekKey] = { clicks: 0, impressions: 0, count: 0 };
      }
      
      weeklyData[weekKey].clicks += day.clicks;
      weeklyData[weekKey].impressions += day.impressions;
      weeklyData[weekKey].count += 1;
    });
    
    return Object.entries(weeklyData)
      .map(([date, values]) => ({
        date,
        clicks: Math.round(values.clicks),
        impressions: Math.round(values.impressions),
        avgClicks: Math.round(values.clicks / values.count),
      }))
      .sort((a, b) => a.date.localeCompare(b.date));
  }, [module1?.traffic_trend?.daily_data]);

  const devicePieData = useMemo(() => {
    if (!module1?.device_breakdown) return [];
    
    const breakdown = module1.device_breakdown;
    return [
      { name: 'Desktop', value: breakdown.desktop.clicks, color: DEVICE_COLORS.desktop },
      { name: 'Mobile', value: breakdown.mobile.clicks, color: DEVICE_COLORS.mobile },
      { name: 'Tablet', value: breakdown.tablet.clicks, color: DEVICE_COLORS.tablet },
    ].filter((d) => d.value > 0);
  }, [module1?.device_breakdown]);

  const topPages = useMemo(() => {
    if (!module1?.top_pages) return [];
    return module1.top_pages.slice(0, 10);
  }, [module1?.top_pages]);

  if (!module1) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <h2 className="text-2xl font-bold text-gray-900 mb-4">Module 1: Traffic Performance</h2>
        <p className="text-gray-600">No performance data available.</p>
      </div>
    );
  }

  const summary = module1.traffic_trend?.summary;

  return (
    <div className="bg-white rounded-lg shadow-md p-6 space-y-8">
      <div>
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Module 1: Traffic Performance</h2>
        <p className="text-gray-600">
          90-day overview of search traffic trends, top-performing pages, and device distribution
        </p>
      </div>

      {/* Summary Stats */}
      {summary && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div className="bg-blue-50 rounded-lg p-4">
            <div className="text-sm text-gray-600 mb-1">Total Clicks</div>
            <div className="text-2xl font-bold text-gray-900">
              {formatNumber(summary.total_clicks_90d)}
            </div>
            {summary.clicks_change_pct !== undefined && (
              <TrendIndicator value={summary.clicks_change_pct} label="vs prev period" />
            )}
          </div>

          <div className="bg-purple-50 rounded-lg p-4">
            <div className="text-sm text-gray-600 mb-1">Total Impressions</div>
            <div className="text-2xl font-bold text-gray-900">
              {formatNumber(summary.total_impressions_90d)}
            </div>
            {summary.impressions_change_pct !== undefined && (
              <TrendIndicator value={summary.impressions_change_pct} label="vs prev period" />
            )}
          </div>

          <div className="bg-green-50 rounded-lg p-4">
            <div className="text-sm text-gray-600 mb-1">Average CTR</div>
            <div className="text-2xl font-bold text-gray-900">
              {formatPercent(summary.avg_ctr_90d)}
            </div>
          </div>

          <div className="bg-orange-50 rounded-lg p-4">
            <div className="text-sm text-gray-600 mb-1">Average Position</div>
            <div className="text-2xl font-bold text-gray-900">
              {summary.avg_position_90d.toFixed(1)}
            </div>
          </div>
        </div>
      )}

      {/* Traffic Trend Chart */}
      {trafficChartData.length > 0 && (
        <div>
          <h3 className="text-lg font-semibold text-gray-900 mb-4">90-Day Traffic Trend</h3>
          <div className="bg-gray-50 rounded-lg p-4">
            <ResponsiveContainer width="100%" height={300}>
              <AreaChart data={trafficChartData}>
                <defs>
                  <linearGradient id="colorClicks" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                  </linearGradient>
                  <linearGradient id="colorImpressions" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0} />
                  </linearGradient>
                </defs>
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
                  stroke="#8b5cf6"
                  style={{ fontSize: '12px' }}
                  tickFormatter={formatNumber}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'white',
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px',
                  }}
                  formatter={(value: number) => formatNumber(value)}
                  labelFormatter={(label) => `Week of ${formatDate(label)}`}
                />
                <Legend />
                <Area
                  yAxisId="left"
                  type="monotone"
                  dataKey="clicks"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  fill="url(#colorClicks)"
                  name="Clicks"
                />
                <Area
                  yAxisId="right"
                  type="monotone"
                  dataKey="impressions"
                  stroke="#8b5cf6"
                  strokeWidth={2}
                  fill="url(#colorImpressions)"
                  name="Impressions"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Top Pages Table */}
        {topPages.length > 0 && (
          <div className="lg:col-span-2">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Top 10 Pages</h3>
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Page
                    </th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Clicks
                    </th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Impressions
                    </th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                      CTR
                    </th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Avg Position
                    </th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Trend
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {topPages.map((page, index) => (
                    <tr key={index} className="hover:bg-gray-50">
                      <td className="px-4 py-3 text-sm text-gray-900 max-w-xs truncate">
                        {page.url}
                      </td>
                      <td className="px-4 py-3 text-sm text-right text-gray-900 font-medium">
                        {formatNumber(page.clicks)}
                      </td>
                      <td className="px-4 py-3 text-sm text-right text-gray-600">
                        {formatNumber(page.impressions)}
                      </td>
                      <td className="px-4 py-3 text-sm text-right text-gray-600">
                        {formatPercent(page.ctr)}
                      </td>
                      <td className="px-4 py-3 text-sm text-right text-gray-600">
                        {page.position.toFixed(1)}
                      </td>
                      <td className="px-4 py-3 text-sm text-right">
                        {page.clicks_change_pct !== undefined ? (
                          <span
                            className={`inline-flex items-center gap-1 ${
                              page.clicks_change_pct > 0
                                ? 'text-green-600'
                                : page.clicks_change_pct < 0
                                ? 'text-red-600'
                                : 'text-gray-600'
                            }`}
                          >
                            {page.clicks_change_pct > 0 ? (
                              <ArrowUpRight className="w-4 h-4" />
                            ) : page.clicks_change_pct < 0 ? (
                              <ArrowDownRight className="w-4 h-4" />
                            ) : (
                              <Minus className="w-4 h-4" />
                            )}
                            {Math.abs(page.clicks_change_pct).toFixed(1)}%
                          </span>
                        ) : (
                          <span className="text-gray-400">-</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Device Breakdown */}
        {devicePieData.length > 0 && module1.device_breakdown && (
          <div>
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Device Distribution</h3>
            <div className="bg-gray-50 rounded-lg p-4">
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={devicePieData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {devicePieData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value: number) => formatNumber(value)} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          </div>
        )}

        {/* Device Stats Table */}
        {module1.device_breakdown && (
          <div>
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Device Performance</h3>
            <div className="space-y-3">
              {Object.entries(module1.device_breakdown).map(([device, stats]) => {
                const Icon =
                  device === 'desktop' ? Monitor : device === 'mobile' ? Smartphone : Tablet;
                const color = DEVICE_COLORS[device as keyof typeof DEVICE_COLORS];

                return (
                  <div
                    key={device}
                    className="bg-gray-50 rounded-lg p-4 flex items-start gap-4"
                  >
                    <div
                      className="p-2 rounded-lg"
                      style={{ backgroundColor: `${color}20` }}
                    >
                      <Icon className="w-6 h-6" style={{ color }} />
                    </div>
                    <div className="flex-1">
                      <div className="font-semibold text-gray-900 capitalize mb-2">
                        {device}
                      </div>
                      <div className="grid grid-cols-2 gap-2 text-sm">
                        <div>
                          <span className="text-gray-600">Clicks:</span>{' '}
                          <span className="font-medium text-gray-900">
                            {formatNumber(stats.clicks)}
                          </span>
                        </div>
                        <div>
                          <span className="text-gray-600">CTR:</span>{' '}
                          <span className="font-medium text-gray-900">
                            {formatPercent(stats.ctr)}
                          </span>
                        </div>
                        <div>
                          <span className="text-gray-600">Impressions:</span>{' '}
                          <span className="font-medium text-gray-900">
                            {formatNumber(stats.impressions)}
                          </span>
                        </div>
                        <div>
                          <span className="text-gray-600">Position:</span>{' '}
                          <span className="font-medium text-gray-900">
                            {stats.position.toFixed(1)}
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default Module1Performance;