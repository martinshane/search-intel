import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { 
  CheckCircle2, 
  XCircle, 
  AlertTriangle, 
  TrendingUp, 
  TrendingDown,
  Minus,
  Globe,
  Lock,
  Smartphone,
  Gauge
} from 'lucide-react';
import { 
  PieChart, 
  Pie, 
  Cell, 
  ResponsiveContainer,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  Radar,
  Tooltip as RechartsTooltip,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Legend
} from 'recharts';

interface Module1Data {
  crawl_stats: {
    total_pages: number;
    crawled: number;
    not_crawled: number;
    crawl_rate_pct: number;
    status: 'healthy' | 'warning' | 'critical';
  };
  indexability: {
    indexable_pct: number;
    blocked_by_robots: number;
    noindex: number;
    canonical_issues: number;
    redirect_chains: number;
    status: 'healthy' | 'warning' | 'critical';
  };
  core_web_vitals: {
    lcp: {
      score: number;
      value: number;
      status: 'good' | 'needs_improvement' | 'poor';
    };
    fid: {
      score: number;
      value: number;
      status: 'good' | 'needs_improvement' | 'poor';
    };
    cls: {
      score: number;
      value: number;
      status: 'good' | 'needs_improvement' | 'poor';
    };
    overall_pass_rate: number;
  };
  mobile_usability: {
    mobile_friendly_pct: number;
    issues_count: number;
    common_issues: Array<{
      type: string;
      count: number;
      severity: 'high' | 'medium' | 'low';
    }>;
    status: 'healthy' | 'warning' | 'critical';
  };
  security_basics: {
    https_pct: number;
    mixed_content_issues: number;
    has_sitemap: boolean;
    sitemap_url?: string;
    sitemap_pages_count?: number;
    robots_txt_exists: boolean;
    robots_txt_valid: boolean;
  };
  structured_data: {
    pages_with_schema: number;
    pages_with_schema_pct: number;
    schema_types: Array<{
      type: string;
      count: number;
    }>;
    validation_errors: number;
  };
  page_speed_distribution: Array<{
    speed_category: string;
    count: number;
  }>;
}

interface Module1TechnicalHealthProps {
  data: Module1Data;
}

const Module1TechnicalHealth: React.FC<Module1TechnicalHealthProps> = ({ data }) => {
  const getStatusColor = (status: 'healthy' | 'warning' | 'critical' | 'good' | 'needs_improvement' | 'poor') => {
    switch (status) {
      case 'healthy':
      case 'good':
        return 'text-green-600 bg-green-50 border-green-200';
      case 'warning':
      case 'needs_improvement':
        return 'text-yellow-600 bg-yellow-50 border-yellow-200';
      case 'critical':
      case 'poor':
        return 'text-red-600 bg-red-50 border-red-200';
      default:
        return 'text-gray-600 bg-gray-50 border-gray-200';
    }
  };

  const getStatusIcon = (status: 'healthy' | 'warning' | 'critical' | 'good' | 'needs_improvement' | 'poor') => {
    switch (status) {
      case 'healthy':
      case 'good':
        return <CheckCircle2 className="h-5 w-5 text-green-600" />;
      case 'warning':
      case 'needs_improvement':
        return <AlertTriangle className="h-5 w-5 text-yellow-600" />;
      case 'critical':
      case 'poor':
        return <XCircle className="h-5 w-5 text-red-600" />;
      default:
        return <Minus className="h-5 w-5 text-gray-600" />;
    }
  };

  const getStatusLabel = (status: 'healthy' | 'warning' | 'critical' | 'good' | 'needs_improvement' | 'poor') => {
    switch (status) {
      case 'healthy':
      case 'good':
        return 'Healthy';
      case 'warning':
      case 'needs_improvement':
        return 'Needs Improvement';
      case 'critical':
      case 'poor':
        return 'Critical';
      default:
        return 'Unknown';
    }
  };

  const getCWVColor = (status: 'good' | 'needs_improvement' | 'poor') => {
    switch (status) {
      case 'good':
        return '#10b981';
      case 'needs_improvement':
        return '#f59e0b';
      case 'poor':
        return '#ef4444';
      default:
        return '#6b7280';
    }
  };

  const indexabilityData = [
    { name: 'Indexable', value: data.indexability.indexable_pct, color: '#10b981' },
    { name: 'Blocked by Robots', value: data.indexability.blocked_by_robots, color: '#ef4444' },
    { name: 'Noindex', value: data.indexability.noindex, color: '#f59e0b' },
    { name: 'Canonical Issues', value: data.indexability.canonical_issues, color: '#3b82f6' },
    { name: 'Redirect Chains', value: data.indexability.redirect_chains, color: '#8b5cf6' }
  ].filter(item => item.value > 0);

  const cwvRadarData = [
    {
      metric: 'LCP',
      score: data.core_web_vitals.lcp.score,
      fullMark: 100
    },
    {
      metric: 'FID',
      score: data.core_web_vitals.fid.score,
      fullMark: 100
    },
    {
      metric: 'CLS',
      score: data.core_web_vitals.cls.score,
      fullMark: 100
    }
  ];

  const pageSpeedData = data.page_speed_distribution.map(item => ({
    category: item.speed_category,
    count: item.count
  }));

  const renderGaugeChart = (value: number, status: 'healthy' | 'warning' | 'critical') => {
    const gaugeData = [
      { name: 'Value', value: value, fill: getCWVColor(status === 'healthy' ? 'good' : status === 'warning' ? 'needs_improvement' : 'poor') },
      { name: 'Remaining', value: 100 - value, fill: '#e5e7eb' }
    ];

    return (
      <ResponsiveContainer width="100%" height={140}>
        <PieChart>
          <Pie
            data={gaugeData}
            cx="50%"
            cy="70%"
            startAngle={180}
            endAngle={0}
            innerRadius={50}
            outerRadius={70}
            paddingAngle={0}
            dataKey="value"
          >
            {gaugeData.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={entry.fill} />
            ))}
          </Pie>
          <text
            x="50%"
            y="65%"
            textAnchor="middle"
            dominantBaseline="middle"
            className="text-3xl font-bold"
          >
            {value}%
          </text>
        </PieChart>
      </ResponsiveContainer>
    );
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h2 className="text-3xl font-bold text-gray-900 mb-2">Technical Health</h2>
        <p className="text-gray-600">
          Core technical foundation and performance metrics for search engine crawling, indexing, and user experience.
        </p>
      </div>

      {/* Overall Health Summary */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Gauge className="h-5 w-5" />
            Health Overview
          </CardTitle>
          <CardDescription>
            Quick snapshot of your site's technical health across key dimensions
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <div className={`p-4 rounded-lg border ${getStatusColor(data.crawl_stats.status)}`}>
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium">Crawlability</span>
                {getStatusIcon(data.crawl_stats.status)}
              </div>
              <div className="text-2xl font-bold">{data.crawl_stats.crawl_rate_pct}%</div>
              <div className="text-xs mt-1">{data.crawl_stats.crawled.toLocaleString()} / {data.crawl_stats.total_pages.toLocaleString()} pages</div>
            </div>

            <div className={`p-4 rounded-lg border ${getStatusColor(data.indexability.status)}`}>
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium">Indexability</span>
                {getStatusIcon(data.indexability.status)}
              </div>
              <div className="text-2xl font-bold">{data.indexability.indexable_pct}%</div>
              <div className="text-xs mt-1">Pages indexable</div>
            </div>

            <div className={`p-4 rounded-lg border ${getStatusColor(
              data.core_web_vitals.overall_pass_rate >= 75 ? 'healthy' : 
              data.core_web_vitals.overall_pass_rate >= 50 ? 'warning' : 'critical'
            )}`}>
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium">Core Web Vitals</span>
                {getStatusIcon(
                  data.core_web_vitals.overall_pass_rate >= 75 ? 'healthy' : 
                  data.core_web_vitals.overall_pass_rate >= 50 ? 'warning' : 'critical'
                )}
              </div>
              <div className="text-2xl font-bold">{data.core_web_vitals.overall_pass_rate}%</div>
              <div className="text-xs mt-1">Pages passing</div>
            </div>

            <div className={`p-4 rounded-lg border ${getStatusColor(data.mobile_usability.status)}`}>
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium">Mobile Usability</span>
                {getStatusIcon(data.mobile_usability.status)}
              </div>
              <div className="text-2xl font-bold">{data.mobile_usability.mobile_friendly_pct}%</div>
              <div className="text-xs mt-1">Mobile friendly</div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Crawl & Index Status */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Globe className="h-5 w-5" />
              Crawl Status
            </CardTitle>
            <CardDescription>
              How easily can search engines discover and crawl your content
            </CardDescription>
          </CardHeader>
          <CardContent>
            {renderGaugeChart(data.crawl_stats.crawl_rate_pct, data.crawl_stats.status)}
            
            <div className="mt-6 space-y-3">
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-600">Total Pages</span>
                <span className="font-semibold">{data.crawl_stats.total_pages.toLocaleString()}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-600">Crawled</span>
                <span className="font-semibold text-green-600">{data.crawl_stats.crawled.toLocaleString()}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-600">Not Crawled</span>
                <span className="font-semibold text-red-600">{data.crawl_stats.not_crawled.toLocaleString()}</span>
              </div>
            </div>

            {data.crawl_stats.status !== 'healthy' && (
              <Alert className="mt-4">
                <AlertTriangle className="h-4 w-4" />
                <AlertDescription>
                  {data.crawl_stats.status === 'critical' 
                    ? 'Critical: Large portions of your site are not being crawled. Check robots.txt and crawl budget optimization.'
                    : 'Some pages are not being crawled efficiently. Consider improving internal linking and reducing crawl blockers.'}
                </AlertDescription>
              </Alert>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Indexability Breakdown</CardTitle>
            <CardDescription>
              Distribution of pages by indexing status and issues
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={200}>
              <PieChart>
                <Pie
                  data={indexabilityData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {indexabilityData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.color} />
                  ))}
                </Pie>
                <RechartsTooltip />
              </PieChart>
            </ResponsiveContainer>

            <div className="mt-6 space-y-2">
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-600">Blocked by Robots</span>
                <Badge variant={data.indexability.blocked_by_robots > 0 ? 'destructive' : 'secondary'}>
                  {data.indexability.blocked_by_robots}
                </Badge>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-600">Noindex Tags</span>
                <Badge variant={data.indexability.noindex > 0 ? 'destructive' : 'secondary'}>
                  {data.indexability.noindex}
                </Badge>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-600">Canonical Issues</span>
                <Badge variant={data.indexability.canonical_issues > 0 ? 'destructive' : 'secondary'}>
                  {data.indexability.canonical_issues}
                </Badge>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-600">Redirect Chains</span>
                <Badge variant={data.indexability.redirect_chains > 0 ? 'destructive' : 'secondary'}>
                  {data.indexability.redirect_chains}
                </Badge>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Core Web Vitals */}
      <Card>
        <CardHeader>
          <CardTitle>Core Web Vitals</CardTitle>
          <CardDescription>
            User experience metrics that directly impact search rankings
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div>
              <ResponsiveContainer width="100%" height={250}>
                <RadarChart data={cwvRadarData}>
                  <PolarGrid />
                  <PolarAngleAxis dataKey="metric" />
                  <PolarRadiusAxis angle={90} domain={[0, 100]} />
                  <Radar
                    name="Score"
                    dataKey="score"
                    stroke="#3b82f6"
                    fill="#3b82f6"
                    fillOpacity={0.6}
                  />
                  <RechartsTooltip />
                </RadarChart>
              </ResponsiveContainer>
            </div>

            <div className="space-y-6">
              {/* LCP */}
              <div>
                <div className="flex items-center justify-between mb-2">
                  <div>
                    <div className="font-semibold">Largest Contentful Paint (LCP)</div>
                    <div className="text-sm text-gray-600">Loading performance</div>
                  </div>
                  <Badge className={getStatusColor(data.core_web_vitals.lcp.status)}>
                    {getStatusLabel(data.core_web_vitals.lcp.status)}
                  </Badge>
                </div>
                <div className="flex items-center gap-3">
                  <Progress 
                    value={data.core_web_vitals.lcp.score} 
                    className="flex-1"
                  />
                  <span className="text-sm font-semibold min-w-[80px] text-right">
                    {data.core_web_vitals.lcp.value.toFixed(2)}s
                  </span>
                </div>
                <div className="text-xs text-gray-500 mt-1">
                  Target: ≤ 2.5s (Good), ≤ 4.0s (Needs Improvement), &gt; 4.0s (Poor)
                </div>
              </div>

              {/* FID */}
              <div>
                <div className="flex items-center justify-between mb-2">
                  <div>
                    <div className="font-semibold">First Input Delay (FID)</div>
                    <div className="text-sm text-gray-600">Interactivity</div>
                  </div>
                  <Badge className={getStatusColor(data.core_web_vitals.fid.status)}>
                    {getStatusLabel(data.core_web_vitals.fid.status)}
                  </Badge>
                </div>
                <div className="flex items-center gap-3">
                  <Progress 
                    value={data.core_web_vitals.fid.score} 
                    className="flex-1"
                  />
                  <span className="text-sm font-semibold min-w-[80px] text-right">
                    {data.core_web_vitals.fid.value.toFixed(0)}ms
                  </span>
                </div>
                <div className="text-xs text-gray-500 mt-1">
                  Target: ≤ 100ms (Good), ≤ 300ms (Needs Improvement), &gt; 300ms (Poor)
                </div>
              </div>

              {/* CLS */}
              <div>
                <div className="flex items-center justify-between mb-2">
                  <div>
                    <div className="font-semibold">Cumulative Layout Shift (CLS)</div>
                    <div className="text-sm text-gray-600">Visual stability</div>
                  </div>
                  <Badge className={getStatusColor(data.core_web_vitals.cls.status)}>
                    {getStatusLabel(data.core_web_vitals.cls.status)}
                  </Badge>
                </div>
                <div className="flex items-center gap-3">
                  <Progress 
                    value={data.core_web_vitals.cls.score} 
                    className="flex-1"
                  />
                  <span className="text-sm font-semibold min-w-[80px] text-right">
                    {data.core_web_vitals.cls.value.toFixed(3)}
                  </span>
                </div>
                <div className="text-xs text-gray-500 mt-1">
                  Target: ≤ 0.1 (Good), ≤ 0.25 (Needs Improvement), &gt; 0.25 (Poor)
                </div>
              </div>
            </div>
          </div>

          <div className="mt-6 p-4 bg-blue-50 rounded-lg border border-blue-200">
            <div className="flex items-start gap-3">
              <AlertTriangle className="h-5 w-5 text-blue-600 mt-0.5" />
              <div>
                <div className="font-semibold text-blue-900">Overall Pass Rate: {data.core_web_vitals.overall_pass_rate}%</div>
                <div className="text-sm text-blue-700 mt-1">
                  {data.core_web_vitals.overall_pass_rate >= 75 
                    ? 'Excellent! Your site meets Core Web Vitals thresholds for most pages.'
                    : data.core_web_vitals.overall_pass_rate >= 50
                    ? 'Moderate performance. Focus on improving your slowest pages to boost rankings.'
                    : 'Critical issue. Poor Core Web Vitals can significantly harm your search visibility.'}
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Mobile Usability & Page Speed */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Smartphone className="h-5 w-5" />
              Mobile Usability
            </CardTitle>
            <CardDescription>
              Mobile-friendliness and responsive design issues
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="mb-6">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm text-gray-600">Mobile Friendly Pages</span>
                <span className="text-2xl font-bold">{data.mobile_usability.mobile_friendly_pct}%</span>
              </div>
              <Progress value={data.mobile_usability.mobile_friendly_pct} className="h-3" />
            </div>

            {data.mobile_usability.issues_count > 0 && (
              <>
                <div className="mb-4">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium">Total Issues</span>
                    <Badge variant="destructive">{data.mobile_usability.issues_count}</Badge>
                  </div>
                </div>

                <div className="space-y-2">
                  <div className="text-sm font-medium mb-2">Common Issues:</div>
                  {data.mobile_usability.common_issues.map((issue, index) => (
                    <div key={index} className="flex items-center justify-between p-2 bg-gray-50 rounded">
                      <div className="flex items-center gap-2">
                        {issue.severity === 'high' && <XCircle className="h-4 w-4 text-red-600" />}
                        {issue.severity === 'medium' && <AlertTriangle className="h-4 w-4 text-yellow-600" />}
                        {issue.severity === 'low' && <Minus className="h-4 w-4 text-gray-600" />}
                        <span className="text-sm">{issue.type.replace(/_/g, ' ')}</span>
                      </div>
                      <Badge variant="secondary">{issue.count}</Badge>
                    </div>
                  ))}
                </div>
              </>
            )}

            {data.mobile_usability.mobile_friendly_pct < 100 && (
              <Alert className="mt-4">
                <AlertDescription>
                  Mobile-first indexing means mobile issues directly impact your rankings for all devices.
                </AlertDescription>
              </Alert>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Page Speed Distribution</CardTitle>
            <CardDescription>
              How your pages are distributed across speed categories
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={pageSpeedData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="category" />
                <YAxis />
                <RechartsTooltip />
                <Bar dataKey="count" fill="#3b82f6" />
              </BarChart>
            </ResponsiveContainer>

            <div className="mt-4 grid grid-cols-3 gap-2">
              {pageSpeedData.map((item, index) => (
                <div key={index} className="text-center p-2 bg-gray-50 rounded">
                  <div className="text-xs text-gray-600">{item.category}</div>
                  <div className="text-lg font-bold">{item.count}</div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Security & Configuration */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Lock className="h-5 w-5" />
            Security & Configuration
          </CardTitle>
          <CardDescription>
            Basic security measures and search engine configuration
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {/* HTTPS */}
            <div className={`p-4 rounded-lg border ${
              data.security_basics.https_pct === 100 
                ? 'bg-green-50 border-green-200' 
                : data.security_basics.https_pct >= 90
                ? 'bg-yellow-50 border-yellow-200'
                : 'bg-red-50 border-red-200'
            }`}>
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium">HTTPS Coverage</span>
                {data.security_basics.https_pct === 100 ? (
                  <CheckCircle2 className="h-5 w-5 text-green-600" />
                ) : (
                  <AlertTriangle className="h-5 w-5 text-yellow-600" />
                )}
              </div>
              <div className="text-2xl font-bold">{data.security_basics.https_pct}%</div>
              {data.security_basics.mixed_content_issues > 0 && (
                <div className="text-xs text-red-600 mt-1">
                  {data.security_basics.mixed_content_issues} mixed content issues
                </div>
              )}
            </div>

            {/* Sitemap */}
            <div className={`p-4 rounded-lg border ${
              data.security_basics.has_sitemap 
                ? 'bg-green-50 border-green-200' 
                : 'bg-red-50 border-red-200'
            }`}>
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium">XML Sitemap</span>
                {data.security_basics.has_sitemap ? (
                  <CheckCircle2 className="h-5 w-5 text-green-600" />
                ) : (
                  <XCircle className="h-5 w-5 text-red-600" />
                )}
              </div>
              <div className="text-lg font-bold">
                {data.security_basics.has_sitemap ? 'Present' : 'Missing'}
              </div>
              {data.security_basics.has_sitemap && data.security_basics.sitemap_pages_count && (
                <div className="text-xs text-gray-600 mt-1">
                  {data.security_basics.sitemap_pages_count.toLocaleString()} URLs listed
                </div>
              )}
            </div>

            {/* Robots.txt */}
            <div className={`p-4 rounded-lg border ${
              data.security_basics.robots_txt_exists && data.security_basics.robots_txt_valid
                ? 'bg-green-50 border-green-200' 
                : data.security_basics.robots_txt_exists
                ? 'bg-yellow-50 border-yellow-200'
                : 'bg-red-50 border-red-200'
            }`}>
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium">Robots.txt</span>
                {data.security_basics.robots_txt_exists && data.security_basics.robots_txt_valid ? (
                  <CheckCircle2 className="h-5 w-5 text-green-600" />
                ) : data.security_basics.robots_txt_exists ? (
                  <AlertTriangle className="h-5 w-5 text-yellow-600" />
                ) : (
                  <XCircle className="h-5 w-5 text-red-600" />
                )}
              </div>
              <div className="text-lg font-bold">
                {!data.security_basics.robots_txt_exists 
                  ? 'Missing' 
                  : data.security_basics.robots_txt_valid 
                  ? 'Valid' 
                  : 'Invalid'}
              </div>
              {data.security_basics.robots_txt_exists && !data.security_basics.robots_txt_valid && (
                <div className="text-xs text-yellow-600 mt-1">Syntax errors detected</div>
              )}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Structured Data */}
      <Card>
        <CardHeader>
          <CardTitle>Structured Data (Schema.org)</CardTitle>
          <CardDescription>
            Schema markup implementation for enhanced search results
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div>
              <div className="mb-4">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm text-gray-600">Pages with Schema</span>
                  <span className="text-2xl font-bold">{data.structured_data.pages_with_schema_pct}%</span>
                </div>
                <Progress value={data.structured_data.pages_with_schema_pct} className="h-3" />
                <div className="text-xs text-gray-600 mt-1">
                  {data.structured_data.pages_with_schema} of {data.crawl_stats.total_pages} pages
                </div>
              </div>

              {data.structured_data.validation_errors > 0 && (
                <Alert>
                  <AlertTriangle className="h-4 w-4" />
                  <AlertDescription>
                    {data.structured_data.validation_errors} validation error{data.structured_data.validation_errors !== 1 ? 's' : ''} detected
                  </AlertDescription>
                </Alert>
              )}
            </div>

            <div>
              <div className="text-sm font-medium mb-3">Schema Types Implemented:</div>
              <div className="space-y-2">
                {data.structured_data.schema_types.length > 0 ? (
                  data.structured_data.schema_types.map((schema, index) => (
                    <div key={index} className="flex items-center justify-between p-2 bg-gray-50 rounded">
                      <span className="text-sm">{schema.type}</span>
                      <Badge variant="secondary">{schema.count} pages</Badge>
                    </div>
                  ))
                ) : (
                  <div className="text-sm text-gray-500 italic">No schema markup detected</div>
                )}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default Module1TechnicalHealth;