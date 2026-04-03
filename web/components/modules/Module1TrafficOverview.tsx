import React, { useEffect, useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, AreaChart, Area } from 'recharts';
import { TrendingUp, TrendingDown, Minus, Activity, Users, Eye, MousePointerClick, AlertCircle, Info, ArrowUp, ArrowDown } from 'lucide-react';
import { fetchModuleData } from '@/lib/api';

interface DailyDataPoint {
  date: string;
  clicks: number;
  impressions: number;
  users: number;
  sessions: number;
  pageviews: number;
  bounce_rate: number;
  avg_session_duration: number;
  ctr: number;
  avg_position: number;
}

interface MetricSummary {
  total: number;
  mom_change_pct: number;
  mom_change_absolute: number;
  wow_change_pct: number;
}

interface Forecast {
  clicks: number;
  ci_low: number;
  ci_high: number;
}

interface ChangePoint {
  date: string;
  magnitude: number;
  direction: 'drop' | 'spike' | 'shift';
  description?: string;
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

interface Module1Data {
  overall_direction: 'strong_growth' | 'growth' | 'flat' | 'decline' | 'strong_decline';
  trend_slope_pct_per_month: number;
  traffic_health_score: number;
  change_points: ChangePoint[];
  seasonality: Seasonality;
  anomalies: Anomaly[];
  forecast: {
    '30d': Forecast;
    '60d': Forecast;
    '90d': Forecast;
  };
  daily_data: DailyDataPoint[];
  metrics_summary: {
    clicks: MetricSummary;
    impressions: MetricSummary;
    users: MetricSummary;
    sessions: MetricSummary;
    pageviews: MetricSummary;
    ctr: MetricSummary;
    avg_position: MetricSummary;
  };
  data_range: {
    start_date: string;
    end_date: string;
    days_analyzed: number;
  };
}

interface Module1Props {
  reportId: string;
}

const Module1TrafficOverview: React.FC<Module1Props> = ({ reportId }) => {
  const [data, setData] = useState<Module1Data | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedMetrics, setSelectedMetrics] = useState<string[]>(['clicks', 'impressions']);

  useEffect(() => {
    const loadData = async () => {
      try {
        setLoading(true);
        setError(null);
        const moduleData = await fetchModuleData(reportId, 'module1');
        setData(moduleData);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load traffic overview data');
        console.error('Error loading Module 1 data:', err);
      } finally {
        setLoading(false);
      }
    };

    loadData();
  }, [reportId]);

  const getDirectionIcon = (direction: string) => {
    switch (direction) {
      case 'strong_growth':
      case 'growth':
        return <TrendingUp className="h-5 w-5 text-green-600" />;
      case 'strong_decline':
      case 'decline':
        return <TrendingDown className="h-5 w-5 text-red-600" />;
      default:
        return <Minus className="h-5 w-5 text-gray-600" />;
    }
  };

  const getDirectionColor = (direction: string): string => {
    switch (direction) {
      case 'strong_growth':
        return 'bg-green-100 text-green-800 border-green-200';
      case 'growth':
        return 'bg-green-50 text-green-700 border-green-100';
      case 'flat':
        return 'bg-gray-100 text-gray-800 border-gray-200';
      case 'decline':
        return 'bg-orange-50 text-orange-700 border-orange-100';
      case 'strong_decline':
        return 'bg-red-100 text-red-800 border-red-200';
      default:
        return 'bg-gray-100 text-gray-800 border-gray-200';
    }
  };

  const getDirectionLabel = (direction: string): string => {
    return direction.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
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
    return (num >= 0 ? '+' : '') + num.toFixed(1) + '%';
  };

  const formatDate = (dateStr: string): string => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  const formatDateLong = (dateStr: string): string => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
  };

  const toggleMetric = (metric: string) => {
    setSelectedMetrics(prev => {
      if (prev.includes(metric)) {
        return prev.filter(m => m !== metric);
      } else {
        return [...prev, metric];
      }
    });
  };

  if (loading) {
    return (
      <div className="space-y-6">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {[1, 2, 3, 4].map(i => (
            <Card key={i}>
              <CardHeader className="pb-2">
                <Skeleton className="h-4 w-24" />
              </CardHeader>
              <CardContent>
                <Skeleton className="h-8 w-32 mb-2" />
                <Skeleton className="h-4 w-20" />
              </CardContent>
            </Card>
          ))}
        </div>
        <Card>
          <CardHeader>
            <Skeleton className="h-6 w-48" />
          </CardHeader>
          <CardContent>
            <Skeleton className="h-[400px] w-full" />
          </CardContent>
        </Card>
      </div>
    );
  }

  if (error) {
    return (
      <Alert variant="destructive">
        <AlertCircle className="h-4 w-4" />
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    );
  }

  if (!data) {
    return (
      <Alert>
        <Info className="h-4 w-4" />
        <AlertDescription>No data available for this report.</AlertDescription>
      </Alert>
    );
  }

  const metricConfigs = [
    { key: 'clicks', label: 'Total Clicks', icon: MousePointerClick, color: '#3b82f6' },
    { key: 'impressions', label: 'Total Impressions', icon: Eye, color: '#8b5cf6' },
    { key: 'users', label: 'Total Users', icon: Users, color: '#10b981' },
    { key: 'ctr', label: 'Average CTR', icon: Activity, color: '#f59e0b', isPercent: true },
  ];

  return (
    <div className="space-y-6">
      {/* Header Section */}
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-3xl font-bold text-gray-900 mb-2">Traffic Overview & Health</h2>
          <p className="text-gray-600">
            Analysis period: {formatDateLong(data.data_range.start_date)} - {formatDateLong(data.data_range.end_date)} ({data.data_range.days_analyzed} days)
          </p>
        </div>
        <Badge className={`${getDirectionColor(data.overall_direction)} border px-4 py-2 text-sm font-semibold`}>
          <span className="flex items-center gap-2">
            {getDirectionIcon(data.overall_direction)}
            {getDirectionLabel(data.overall_direction)}
          </span>
        </Badge>
      </div>

      {/* Traffic Health Score */}
      <Card className="border-l-4 border-l-blue-600">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Activity className="h-5 w-5" />
            Traffic Health Score
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-6">
            <div className="text-5xl font-bold text-blue-600">
              {data.traffic_health_score.toFixed(1)}
              <span className="text-2xl text-gray-400">/100</span>
            </div>
            <div className="flex-1">
              <div className="w-full bg-gray-200 rounded-full h-4 mb-2">
                <div
                  className="bg-blue-600 h-4 rounded-full transition-all duration-500"
                  style={{ width: `${data.traffic_health_score}%` }}
                />
              </div>
              <p className="text-sm text-gray-600">
                Trend: <strong>{formatPercent(data.trend_slope_pct_per_month)}</strong> per month
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Key Metrics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {metricConfigs.map(({ key, label, icon: Icon, color, isPercent }) => {
          const summary = data.metrics_summary[key as keyof typeof data.metrics_summary];
          if (!summary) return null;

          const changeValue = summary.mom_change_pct;
          const isPositive = changeValue > 0;
          const isNegative = changeValue < 0;

          return (
            <Card key={key} className="hover:shadow-lg transition-shadow">
              <CardHeader className="pb-2">
                <CardDescription className="flex items-center gap-2 text-xs font-medium">
                  <Icon className="h-4 w-4" style={{ color }} />
                  {label}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-gray-900 mb-1">
                  {isPercent ? `${(summary.total * 100).toFixed(2)}%` : formatNumber(summary.total)}
                </div>
                <div className="flex items-center gap-2 text-sm">
                  {isPositive && (
                    <span className="flex items-center text-green-600 font-medium">
                      <ArrowUp className="h-3 w-3" />
                      {formatPercent(Math.abs(changeValue))}
                    </span>
                  )}
                  {isNegative && (
                    <span className="flex items-center text-red-600 font-medium">
                      <ArrowDown className="h-3 w-3" />
                      {formatPercent(Math.abs(changeValue))}
                    </span>
                  )}
                  {!isPositive && !isNegative && (
                    <span className="flex items-center text-gray-600 font-medium">
                      <Minus className="h-3 w-3" />
                      {formatPercent(0)}
                    </span>
                  )}
                  <span className="text-gray-500">vs last month</span>
                </div>
                <div className="text-xs text-gray-400 mt-1">
                  WoW: {formatPercent(summary.wow_change_pct)}
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>

      {/* Time Series Chart */}
      <Card>
        <CardHeader>
          <CardTitle>Traffic Trends Over Time</CardTitle>
          <CardDescription>Daily performance metrics with trend analysis</CardDescription>
          <div className="flex flex-wrap gap-2 mt-4">
            {[
              { key: 'clicks', label: 'Clicks', color: '#3b82f6' },
              { key: 'impressions', label: 'Impressions', color: '#8b5cf6' },
              { key: 'users', label: 'Users', color: '#10b981' },
              { key: 'sessions', label: 'Sessions', color: '#f59e0b' },
            ].map(metric => (
              <button
                key={metric.key}
                onClick={() => toggleMetric(metric.key)}
                className={`px-3 py-1 rounded-md text-sm font-medium transition-colors ${
                  selectedMetrics.includes(metric.key)
                    ? 'bg-blue-100 text-blue-700 border border-blue-300'
                    : 'bg-gray-100 text-gray-600 border border-gray-300 hover:bg-gray-200'
                }`}
              >
                <span className="inline-block w-3 h-3 rounded-full mr-2" style={{ backgroundColor: metric.color }} />
                {metric.label}
              </button>
            ))}
          </div>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={400}>
            <LineChart data={data.daily_data} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="date"
                tickFormatter={formatDate}
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
              />
              <YAxis stroke="#6b7280" style={{ fontSize: '12px' }} tickFormatter={formatNumber} />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'white',
                  border: '1px solid #e5e7eb',
                  borderRadius: '8px',
                  padding: '12px',
                }}
                labelFormatter={(label) => formatDateLong(label)}
                formatter={(value: number) => [formatNumber(value), '']}
              />
              <Legend />
              {selectedMetrics.includes('clicks') && (
                <Line
                  type="monotone"
                  dataKey="clicks"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  dot={false}
                  name="Clicks"
                />
              )}
              {selectedMetrics.includes('impressions') && (
                <Line
                  type="monotone"
                  dataKey="impressions"
                  stroke="#8b5cf6"
                  strokeWidth={2}
                  dot={false}
                  name="Impressions"
                />
              )}
              {selectedMetrics.includes('users') && (
                <Line
                  type="monotone"
                  dataKey="users"
                  stroke="#10b981"
                  strokeWidth={2}
                  dot={false}
                  name="Users"
                />
              )}
              {selectedMetrics.includes('sessions') && (
                <Line
                  type="monotone"
                  dataKey="sessions"
                  stroke="#f59e0b"
                  strokeWidth={2}
                  dot={false}
                  name="Sessions"
                />
              )}
            </LineChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      {/* Forecast Section */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {(['30d', '60d', '90d'] as const).map(period => {
          const forecast = data.forecast[period];
          const days = period === '30d' ? 30 : period === '60d' ? 60 : 90;
          
          return (
            <Card key={period}>
              <CardHeader className="pb-2">
                <CardTitle className="text-lg">{days}-Day Forecast</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-3xl font-bold text-gray-900 mb-2">
                  {formatNumber(forecast.clicks)}
                </div>
                <div className="text-sm text-gray-600 mb-3">
                  Expected clicks
                </div>
                <div className="bg-gray-50 rounded-lg p-3 border border-gray-200">
                  <div className="text-xs text-gray-500 mb-1">Confidence Interval (95%)</div>
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-700">
                      Low: <strong>{formatNumber(forecast.ci_low)}</strong>
                    </span>
                    <span className="text-gray-700">
                      High: <strong>{formatNumber(forecast.ci_high)}</strong>
                    </span>
                  </div>
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>

      {/* Seasonality Insights */}
      {data.seasonality && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Activity className="h-5 w-5" />
              Seasonality Patterns
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <h4 className="font-semibold text-gray-900 mb-2">Day of Week Performance</h4>
                <div className="space-y-2">
                  <div className="flex items-center justify-between p-3 bg-green-50 rounded-lg border border-green-200">
                    <span className="text-sm text-gray-700">Best Day</span>
                    <Badge className="bg-green-100 text-green-800 border-green-200">
                      {data.seasonality.best_day}
                    </Badge>
                  </div>
                  <div className="flex items-center justify-between p-3 bg-red-50 rounded-lg border border-red-200">
                    <span className="text-sm text-gray-700">Worst Day</span>
                    <Badge className="bg-red-100 text-red-800 border-red-200">
                      {data.seasonality.worst_day}
                    </Badge>
                  </div>
                </div>
              </div>
              <div>
                <h4 className="font-semibold text-gray-900 mb-2">Monthly Patterns</h4>
                <div className="p-4 bg-blue-50 rounded-lg border border-blue-200">
                  {data.seasonality.monthly_cycle ? (
                    <>
                      <div className="flex items-center gap-2 mb-2">
                        <Badge className="bg-blue-100 text-blue-800 border-blue-200">
                          Detected
                        </Badge>
                        <span className="text-sm font-medium text-blue-900">Monthly cycle present</span>
                      </div>
                      <p className="text-sm text-gray-700">{data.seasonality.cycle_description}</p>
                    </>
                  ) : (
                    <p className="text-sm text-gray-700">No significant monthly patterns detected</p>
                  )}
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Change Points */}
      {data.change_points && data.change_points.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <AlertCircle className="h-5 w-5" />
              Significant Traffic Changes
            </CardTitle>
            <CardDescription>
              Detected structural breaks in traffic patterns
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {data.change_points.map((cp, idx) => (
                <div
                  key={idx}
                  className={`p-4 rounded-lg border-l-4 ${
                    cp.direction === 'drop'
                      ? 'bg-red-50 border-red-500'
                      : cp.direction === 'spike'
                      ? 'bg-green-50 border-green-500'
                      : 'bg-blue-50 border-blue-500'
                  }`}
                >
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-1">
                        <span className="font-semibold text-gray-900">{formatDateLong(cp.date)}</span>
                        <Badge
                          className={
                            cp.direction === 'drop'
                              ? 'bg-red-100 text-red-800 border-red-200'
                              : cp.direction === 'spike'
                              ? 'bg-green-100 text-green-800 border-green-200'
                              : 'bg-blue-100 text-blue-800 border-blue-200'
                          }
                        >
                          {cp.direction}
                        </Badge>
                      </div>
                      <p className="text-sm text-gray-700">
                        {cp.description || `Traffic ${cp.direction} of ${(Math.abs(cp.magnitude) * 100).toFixed(1)}%`}
                      </p>
                    </div>
                    <div className="text-right">
                      <div className={`text-2xl font-bold ${
                        cp.direction === 'drop' ? 'text-red-600' : cp.direction === 'spike' ? 'text-green-600' : 'text-blue-600'
                      }`}>
                        {cp.magnitude > 0 ? '+' : ''}{(cp.magnitude * 100).toFixed(1)}%
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Anomalies */}
      {data.anomalies && data.anomalies.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <AlertCircle className="h-5 w-5" />
              Traffic Anomalies
            </CardTitle>
            <CardDescription>
              Unusual patterns and one-off events detected
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {data.anomalies.slice(0, 5).map((anomaly, idx) => (
                <div
                  key={idx}
                  className={`p-4 rounded-lg border ${
                    anomaly.type === 'discord'
                      ? 'bg-orange-50 border-orange-200'
                      : 'bg-purple-50 border-purple-200'
                  }`}
                >
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-1">
                        <span className="font-semibold text-gray-900">{formatDateLong(anomaly.date)}</span>
                        <Badge
                          className={
                            anomaly.type === 'discord'
                              ? 'bg-orange-100 text-orange-800 border-orange-200'
                              : 'bg-purple-100 text-purple-800 border-purple-200'
                          }
                        >
                          {anomaly.type === 'discord' ? 'One-off Event' : 'Recurring Pattern'}
                        </Badge>
                      </div>
                      <p className="text-sm text-gray-700">
                        {anomaly.description || `Anomaly detected with magnitude ${(Math.abs(anomaly.magnitude) * 100).toFixed(1)}%`}
                      </p>
                    </div>
                    <div className="text-right">
                      <div className={`text-xl font-bold ${
                        anomaly.type === 'discord' ? 'text-orange-600' : 'text-purple-600'
                      }`}>
                        {(Math.abs(anomaly.magnitude) * 100).toFixed(1)}%
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Data Table */}
      <Card>
        <CardHeader>
          <CardTitle>Daily Breakdown</CardTitle>
          <CardDescription>Detailed metrics by date (most recent 30 days)</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left py-3 px-4 font-semibold text-gray-700">Date</th>
                  <th className="text-right py-3 px-4 font-semibold text-gray-700">Clicks</th>
                  <th className="text-right py-3 px-4 font-semibold text-gray-700">Impressions</th>
                  <th className="text-right py-3 px-4 font-semibold text-gray-700">CTR</th>
                  <th className="text-right py-3 px-4 font-semibold text-gray-700">Avg Position</th>
                  <th className="text-right py-3 px-4 font-semibold text-gray-700">Users</th>
                  <th className="text-right py-3 px-4 font-semibold text-gray-700">Sessions</th>
                </tr>
              </thead>
              <tbody>
                {data.daily_data.slice(-30).reverse().map((day, idx) => (
                  <tr key={idx} className="border-b border-gray-100 hover:bg-gray-50">
                    <td className="py-3 px-4 font-medium text-gray-900">{formatDateLong(day.date)}</td>
                    <td className="py-3 px-4 text-right text-gray-700">{formatNumber(day.clicks)}</td>
                    <td className="py-3 px-4 text-right text-gray-700">{formatNumber(day.impressions)}</td>
                    <td className="py-3 px-4 text-right text-gray-700">{(day.ctr * 100).toFixed(2)}%</td>
                    <td className="py-3 px-4 text-right text-gray-700">{day.avg_position.toFixed(1)}</td>
                    <td className="py-3 px-4 text-right text-gray-700">{formatNumber(day.users)}</td>
                    <td className="py-3 px-4 text-right text-gray-700">{formatNumber(day.sessions)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default Module1TrafficOverview;
