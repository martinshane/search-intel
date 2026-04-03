import React, { useEffect, useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Area, AreaChart } from 'recharts';
import { TrendingUp, TrendingDown, Minus, Activity, Users, Eye, MousePointerClick, AlertCircle, Info } from 'lucide-react';
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
  const [selectedMetrics, setSelectedMetrics] = useState<string[]>(['clicks', 'users']);

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

  const getDirectionColor = (direction: string) => {
    switch (direction) {
      case 'strong_growth':
        return 'text-green-700 bg-green-50 border-green-200';
      case 'growth':
        return 'text-green-600 bg-green-50 border-green-200';
      case 'strong_decline':
        return 'text-red-700 bg-red-50 border-red-200';
      case 'decline':
        return 'text-red-600 bg-red-50 border-red-200';
      default:
        return 'text-gray-600 bg-gray-50 border-gray-200';
    }
  };

  const getDirectionLabel = (direction: string) => {
    const labels: Record<string, string> = {
      strong_growth: 'Strong Growth',
      growth: 'Growing',
      flat: 'Stable',
      decline: 'Declining',
      strong_decline: 'Strong Decline'
    };
    return labels[direction] || direction;
  };

  const getHealthScoreColor = (score: number) => {
    if (score >= 80) return 'text-green-600';
    if (score >= 60) return 'text-yellow-600';
    if (score >= 40) return 'text-orange-600';
    return 'text-red-600';
  };

  const getHealthScoreBadge = (score: number) => {
    if (score >= 80) return { label: 'Excellent', variant: 'default' as const, color: 'bg-green-500' };
    if (score >= 60) return { label: 'Good', variant: 'secondary' as const, color: 'bg-yellow-500' };
    if (score >= 40) return { label: 'Fair', variant: 'secondary' as const, color: 'bg-orange-500' };
    return { label: 'Poor', variant: 'destructive' as const, color: 'bg-red-500' };
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
    const sign = num > 0 ? '+' : '';
    return `${sign}${num.toFixed(1)}%`;
  };

  const formatDate = (dateStr: string): string => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  const prepareChartData = () => {
    if (!data) return [];
    
    return data.daily_data.map(point => ({
      date: formatDate(point.date),
      fullDate: point.date,
      clicks: point.clicks,
      impressions: point.impressions,
      users: point.users,
      sessions: point.sessions,
      pageviews: point.pageviews
    }));
  };

  const MetricCard: React.FC<{
    title: string;
    icon: React.ReactNode;
    value: number;
    change: number;
    changeLabel?: string;
  }> = ({ title, icon, value, change, changeLabel = 'vs last month' }) => {
    const isPositive = change > 0;
    const isNegative = change < 0;
    
    return (
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">{title}</CardTitle>
          {icon}
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{formatNumber(value)}</div>
          <div className="flex items-center text-xs text-muted-foreground mt-1">
            {isPositive && <TrendingUp className="h-3 w-3 text-green-600 mr-1" />}
            {isNegative && <TrendingDown className="h-3 w-3 text-red-600 mr-1" />}
            <span className={isPositive ? 'text-green-600' : isNegative ? 'text-red-600' : ''}>
              {formatPercentage(change)}
            </span>
            <span className="ml-1">{changeLabel}</span>
          </div>
        </CardContent>
      </Card>
    );
  };

  if (loading) {
    return (
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <Skeleton className="h-8 w-64 mb-2" />
            <Skeleton className="h-4 w-96" />
          </div>
          <Skeleton className="h-20 w-20 rounded-full" />
        </div>
        
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          {[...Array(4)].map((_, i) => (
            <Card key={i}>
              <CardHeader>
                <Skeleton className="h-4 w-24" />
              </CardHeader>
              <CardContent>
                <Skeleton className="h-8 w-32 mb-2" />
                <Skeleton className="h-3 w-24" />
              </CardContent>
            </Card>
          ))}
        </div>

        <Card>
          <CardHeader>
            <Skeleton className="h-6 w-48" />
          </CardHeader>
          <CardContent>
            <Skeleton className="h-80 w-full" />
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
    return null;
  }

  const healthBadge = getHealthScoreBadge(data.traffic_health_score);
  const chartData = prepareChartData();

  return (
    <div className="space-y-6">
      {/* Header with Health Score */}
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-3xl font-bold tracking-tight">Traffic Health & Trajectory</h2>
          <p className="text-muted-foreground mt-2">
            Analysis of {data.data_range.days_analyzed} days from {formatDate(data.data_range.start_date)} to {formatDate(data.data_range.end_date)}
          </p>
          <div className="flex items-center gap-3 mt-4">
            <div className={`flex items-center gap-2 px-3 py-1.5 rounded-lg border ${getDirectionColor(data.overall_direction)}`}>
              {getDirectionIcon(data.overall_direction)}
              <span className="font-semibold">{getDirectionLabel(data.overall_direction)}</span>
              <span className="text-sm">({formatPercentage(data.trend_slope_pct_per_month)}/month)</span>
            </div>
          </div>
        </div>
        
        {/* Traffic Health Score Circle */}
        <div className="flex flex-col items-center">
          <div className="relative w-24 h-24">
            <svg className="transform -rotate-90 w-24 h-24">
              <circle
                cx="48"
                cy="48"
                r="40"
                stroke="currentColor"
                strokeWidth="8"
                fill="none"
                className="text-gray-200"
              />
              <circle
                cx="48"
                cy="48"
                r="40"
                stroke="currentColor"
                strokeWidth="8"
                fill="none"
                strokeDasharray={`${2 * Math.PI * 40}`}
                strokeDashoffset={`${2 * Math.PI * 40 * (1 - data.traffic_health_score / 100)}`}
                className={getHealthScoreColor(data.traffic_health_score)}
                strokeLinecap="round"
              />
            </svg>
            <div className="absolute inset-0 flex items-center justify-center">
              <span className={`text-2xl font-bold ${getHealthScoreColor(data.traffic_health_score)}`}>
                {data.traffic_health_score}
              </span>
            </div>
          </div>
          <Badge variant={healthBadge.variant} className={`mt-2 ${healthBadge.color}`}>
            {healthBadge.label}
          </Badge>
          <span className="text-xs text-muted-foreground mt-1">Health Score</span>
        </div>
      </div>

      {/* Key Metrics Cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <MetricCard
          title="Total Clicks"
          icon={<MousePointerClick className="h-4 w-4 text-muted-foreground" />}
          value={data.metrics_summary.clicks.total}
          change={data.metrics_summary.clicks.mom_change_pct}
        />
        <MetricCard
          title="Total Impressions"
          icon={<Eye className="h-4 w-4 text-muted-foreground" />}
          value={data.metrics_summary.impressions.total}
          change={data.metrics_summary.impressions.mom_change_pct}
        />
        <MetricCard
          title="Users"
          icon={<Users className="h-4 w-4 text-muted-foreground" />}
          value={data.metrics_summary.users.total}
          change={data.metrics_summary.users.mom_change_pct}
        />
        <MetricCard
          title="Sessions"
          icon={<Activity className="h-4 w-4 text-muted-foreground" />}
          value={data.metrics_summary.sessions.total}
          change={data.metrics_summary.sessions.mom_change_pct}
        />
      </div>

      {/* Main Traffic Chart */}
      <Card>
        <CardHeader>
          <CardTitle>Traffic Trends</CardTitle>
          <CardDescription>
            Daily performance metrics over time
          </CardDescription>
          <div className="flex gap-2 mt-4">
            {['clicks', 'impressions', 'users', 'sessions', 'pageviews'].map((metric) => (
              <Badge
                key={metric}
                variant={selectedMetrics.includes(metric) ? 'default' : 'outline'}
                className="cursor-pointer"
                onClick={() => {
                  if (selectedMetrics.includes(metric)) {
                    setSelectedMetrics(selectedMetrics.filter(m => m !== metric));
                  } else {
                    setSelectedMetrics([...selectedMetrics, metric]);
                  }
                }}
              >
                {metric.charAt(0).toUpperCase() + metric.slice(1)}
              </Badge>
            ))}
          </div>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={400}>
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="colorClicks" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3}/>
                  <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
                </linearGradient>
                <linearGradient id="colorImpressions" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.3}/>
                  <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0}/>
                </linearGradient>
                <linearGradient id="colorUsers" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#10b981" stopOpacity={0.3}/>
                  <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                </linearGradient>
                <linearGradient id="colorSessions" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#f59e0b" stopOpacity={0.3}/>
                  <stop offset="95%" stopColor="#f59e0b" stopOpacity={0}/>
                </linearGradient>
                <linearGradient id="colorPageviews" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#ec4899" stopOpacity={0.3}/>
                  <stop offset="95%" stopColor="#ec4899" stopOpacity={0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis 
                dataKey="date" 
                tick={{ fontSize: 12 }}
                tickLine={false}
              />
              <YAxis 
                tick={{ fontSize: 12 }}
                tickLine={false}
                tickFormatter={(value) => formatNumber(value)}
              />
              <Tooltip 
                contentStyle={{ 
                  backgroundColor: 'hsl(var(--background))',
                  border: '1px solid hsl(var(--border))',
                  borderRadius: '6px'
                }}
                formatter={(value: number) => formatNumber(value)}
              />
              <Legend />
              {selectedMetrics.includes('clicks') && (
                <Area
                  type="monotone"
                  dataKey="clicks"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  fill="url(#colorClicks)"
                  name="Clicks"
                />
              )}
              {selectedMetrics.includes('impressions') && (
                <Area
                  type="monotone"
                  dataKey="impressions"
                  stroke="#8b5cf6"
                  strokeWidth={2}
                  fill="url(#colorImpressions)"
                  name="Impressions"
                />
              )}
              {selectedMetrics.includes('users') && (
                <Area
                  type="monotone"
                  dataKey="users"
                  stroke="#10b981"
                  strokeWidth={2}
                  fill="url(#colorUsers)"
                  name="Users"
                />
              )}
              {selectedMetrics.includes('sessions') && (
                <Area
                  type="monotone"
                  dataKey="sessions"
                  stroke="#f59e0b"
                  strokeWidth={2}
                  fill="url(#colorSessions)"
                  name="Sessions"
                />
              )}
              {selectedMetrics.includes('pageviews') && (
                <Area
                  type="monotone"
                  dataKey="pageviews"
                  stroke="#ec4899"
                  strokeWidth={2}
                  fill="url(#colorPageviews)"
                  name="Pageviews"
                />
              )}
            </AreaChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      <div className="grid gap-6 md:grid-cols-2">
        {/* Change Points */}
        {data.change_points.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle>Significant Changes Detected</CardTitle>
              <CardDescription>
                Structural shifts in traffic patterns
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {data.change_points.map((point, idx) => (
                  <div key={idx} className="flex items-start gap-3 p-3 rounded-lg bg-muted/50">
                    {point.direction === 'drop' && <TrendingDown className="h-5 w-5 text-red-600 mt-0.5" />}
                    {point.direction === 'spike' && <TrendingUp className="h-5 w-5 text-green-600 mt-0.5" />}
                    {point.direction === 'shift' && <Activity className="h-5 w-5 text-blue-600 mt-0.5" />}
                    <div className="flex-1">
                      <div className="font-semibold text-sm">{formatDate(point.date)}</div>
                      <div className="text-sm text-muted-foreground">
                        {Math.abs(point.magnitude * 100).toFixed(1)}% {point.direction}
                      </div>
                      {point.description && (
                        <div className="text-xs text-muted-foreground mt-1">{point.description}</div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Seasonality Insights */}
        <Card>
          <CardHeader>
            <CardTitle>Seasonality Patterns</CardTitle>
            <CardDescription>
              Recurring traffic patterns detected
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <div>
                <div className="text-sm font-medium mb-2">Day of Week Pattern</div>
                <div className="flex items-center justify-between text-sm">
                  <div className="flex items-center gap-2">
                    <TrendingUp className="h-4 w-4 text-green-600" />
                    <span>Best: {data.seasonality.best_day}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <TrendingDown className="h-4 w-4 text-red-600" />
                    <span>Worst: {data.seasonality.worst_day}</span>
                  </div>
                </div>
              </div>

              {data.seasonality.monthly_cycle && (
                <div>
                  <div className="text-sm font-medium mb-2">Monthly Cycle</div>
                  <p className="text-sm text-muted-foreground">
                    {data.seasonality.cycle_description}
                  </p>
                </div>
              )}

              <Alert>
                <Info className="h-4 w-4" />
                <AlertDescription className="text-sm">
                  Use these patterns to optimize content publishing and promotional timing.
                </AlertDescription>
              </Alert>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Forecast */}
      <Card>
        <CardHeader>
          <CardTitle>Traffic Forecast</CardTitle>
          <CardDescription>
            Projected performance based on current trends
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-3">
            {(['30d', '60d', '90d'] as const).map((period) => {
              const forecast = data.forecast[period];
              const days = period === '30d' ? 30 : period === '60d' ? 60 : 90;
              const currentClicks = data.metrics_summary.clicks.total;
              const projectedChange = ((forecast.clicks - currentClicks) / currentClicks) * 100;
              
              return (
                <div key={period} className="p-4 rounded-lg border bg-card">
                  <div className="text-sm font-medium text-muted-foreground mb-2">
                    {days} Days
                  </div>
                  <div className="text-2xl font-bold mb-1">
                    {formatNumber(forecast.clicks)}
                  </div>
                  <div className="text-sm text-muted-foreground mb-3">
                    clicks (±{formatNumber(forecast.ci_high - forecast.clicks)})
                  </div>
                  <div className="flex items-center text-sm">
                    {projectedChange > 0 ? (
                      <TrendingUp className="h-4 w-4 text-green-600 mr-1" />
                    ) : (
                      <TrendingDown className="h-4 w-4 text-red-600 mr-1" />
                    )}
                    <span className={projectedChange > 0 ? 'text-green-600' : 'text-red-600'}>
                      {formatPercentage(projectedChange)}
                    </span>
                  </div>
                  <div className="mt-3 pt-3 border-t text-xs text-muted-foreground">
                    Range: {formatNumber(forecast.ci_low)} - {formatNumber(forecast.ci_high)}
                  </div>
                </div>
              );
            })}
          </div>
        </CardContent>
      </Card>

      {/* Anomalies */}
      {data.anomalies.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Traffic Anomalies</CardTitle>
            <CardDescription>
              Unusual patterns and one-off events detected
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {data.anomalies.map((anomaly, idx) => (
                <div key={idx} className="flex items-center justify-between p-3 rounded-lg border">
                  <div className="flex items-center gap-3">
                    <AlertCircle className="h-5 w-5 text-yellow-600" />
                    <div>
                      <div className="font-semibold text-sm">{formatDate(anomaly.date)}</div>
                      <div className="text-sm text-muted-foreground">
                        {anomaly.type === 'discord' ? 'One-off anomaly' : 'Recurring pattern'}
                      </div>
                      {anomaly.description && (
                        <div className="text-xs text-muted-foreground mt-1">{anomaly.description}</div>
                      )}
                    </div>
                  </div>
                  <Badge variant="outline">
                    {Math.abs(anomaly.magnitude * 100).toFixed(0)}% deviation
                  </Badge>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
};

export default Module1TrafficOverview;
