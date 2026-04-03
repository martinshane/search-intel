import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Skeleton } from '@/components/ui/skeleton';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { BarChart, Bar, ScatterChart, Scatter, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell, PieChart, Pie } from 'recharts';
import { ArrowUpIcon, ArrowDownIcon, MinusIcon, TrendingUp, TrendingDown, Eye, MousePointer, Target, AlertTriangle } from 'lucide-react';

interface Module1Data {
  overall_direction: string;
  trend_slope_pct_per_month: number;
  change_points: Array<{
    date: string;
    magnitude: number;
    direction: string;
  }>;
  seasonality: {
    best_day: string;
    worst_day: string;
    monthly_cycle: boolean;
    cycle_description: string;
  };
  anomalies: Array<{
    date: string;
    type: string;
    magnitude: number;
  }>;
  forecast: {
    '30d': { clicks: number; ci_low: number; ci_high: number };
    '60d': { clicks: number; ci_low: number; ci_high: number };
    '90d': { clicks: number; ci_low: number; ci_high: number };
  };
  top_queries: Array<{
    query: string;
    impressions: number;
    clicks: number;
    ctr: number;
    position: number;
    trend: 'up' | 'down' | 'stable';
    trend_pct: number;
  }>;
  query_distribution: Array<{
    bucket: string;
    impressions: number;
    clicks: number;
    query_count: number;
  }>;
  position_distribution: Array<{
    position_range: string;
    query_count: number;
    avg_ctr: number;
    total_clicks: number;
  }>;
}

interface Module1QueryVisibilityProps {
  data: Module1Data | null;
  loading: boolean;
  error: string | null;
}

const Module1QueryVisibility: React.FC<Module1QueryVisibilityProps> = ({ data, loading, error }) => {
  const [sortColumn, setSortColumn] = React.useState<'impressions' | 'clicks' | 'ctr' | 'position'>('impressions');
  const [sortDirection, setSortDirection] = React.useState<'asc' | 'desc'>('desc');

  if (loading) {
    return (
      <Card className="w-full">
        <CardHeader>
          <Skeleton className="h-8 w-64" />
          <Skeleton className="h-4 w-96 mt-2" />
        </CardHeader>
        <CardContent className="space-y-6">
          <Skeleton className="h-64 w-full" />
          <Skeleton className="h-64 w-full" />
          <Skeleton className="h-64 w-full" />
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Card className="w-full">
        <CardHeader>
          <CardTitle>Query Visibility Analysis</CardTitle>
          <CardDescription>Search query performance and distribution</CardDescription>
        </CardHeader>
        <CardContent>
          <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertTitle>Error Loading Data</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    );
  }

  if (!data) {
    return (
      <Card className="w-full">
        <CardHeader>
          <CardTitle>Query Visibility Analysis</CardTitle>
          <CardDescription>Search query performance and distribution</CardDescription>
        </CardHeader>
        <CardContent>
          <Alert>
            <AlertTitle>No Data Available</AlertTitle>
            <AlertDescription>No query visibility data is available for this report.</AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    );
  }

  const handleSort = (column: 'impressions' | 'clicks' | 'ctr' | 'position') => {
    if (sortColumn === column) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(column);
      setSortDirection('desc');
    }
  };

  const sortedQueries = [...(data.top_queries || [])].sort((a, b) => {
    const multiplier = sortDirection === 'asc' ? 1 : -1;
    return (a[sortColumn] - b[sortColumn]) * multiplier;
  });

  const getTrendIcon = (trend: 'up' | 'down' | 'stable') => {
    if (trend === 'up') return <ArrowUpIcon className="h-4 w-4 text-green-600" />;
    if (trend === 'down') return <ArrowDownIcon className="h-4 w-4 text-red-600" />;
    return <MinusIcon className="h-4 w-4 text-gray-400" />;
  };

  const getTrendBadgeVariant = (trend: 'up' | 'down' | 'stable') => {
    if (trend === 'up') return 'default';
    if (trend === 'down') return 'destructive';
    return 'secondary';
  };

  const formatNumber = (num: number, decimals: number = 0): string => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toFixed(decimals);
  };

  const formatPercentage = (num: number): string => {
    return (num * 100).toFixed(2) + '%';
  };

  const getCtrColor = (ctr: number, avgPosition: number): string => {
    // Expected CTR based on position (rough estimates)
    const expectedCtr: { [key: number]: number } = {
      1: 0.30, 2: 0.15, 3: 0.10, 4: 0.07, 5: 0.05,
      6: 0.04, 7: 0.03, 8: 0.025, 9: 0.02, 10: 0.015
    };
    
    const positionKey = Math.min(Math.round(avgPosition), 10);
    const expected = expectedCtr[positionKey] || 0.01;
    
    if (ctr >= expected * 1.2) return '#10b981'; // green
    if (ctr >= expected * 0.8) return '#6b7280'; // gray
    return '#ef4444'; // red
  };

  // Prepare scatter plot data
  const scatterData = (data.top_queries || []).map(q => ({
    ctr: q.ctr * 100,
    position: q.position,
    query: q.query,
    clicks: q.clicks,
    impressions: q.impressions
  }));

  // Position distribution colors
  const positionColors = ['#3b82f6', '#8b5cf6', '#ec4899', '#f59e0b', '#ef4444'];

  return (
    <Card className="w-full">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Eye className="h-6 w-6" />
          Query Visibility Analysis
        </CardTitle>
        <CardDescription>
          Comprehensive analysis of search query performance, distribution, and click-through patterns
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Tabs defaultValue="queries" className="w-full">
          <TabsList className="grid w-full grid-cols-4">
            <TabsTrigger value="queries">Top Queries</TabsTrigger>
            <TabsTrigger value="distribution">Distribution</TabsTrigger>
            <TabsTrigger value="ctr">CTR Analysis</TabsTrigger>
            <TabsTrigger value="positions">Positions</TabsTrigger>
          </TabsList>

          {/* Top Queries Table */}
          <TabsContent value="queries" className="space-y-4">
            <div className="rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-[40%]">Query</TableHead>
                    <TableHead 
                      className="cursor-pointer hover:bg-muted/50"
                      onClick={() => handleSort('impressions')}
                    >
                      <div className="flex items-center gap-1">
                        <Eye className="h-4 w-4" />
                        Impressions
                        {sortColumn === 'impressions' && (
                          sortDirection === 'desc' ? <ArrowDownIcon className="h-3 w-3" /> : <ArrowUpIcon className="h-3 w-3" />
                        )}
                      </div>
                    </TableHead>
                    <TableHead 
                      className="cursor-pointer hover:bg-muted/50"
                      onClick={() => handleSort('clicks')}
                    >
                      <div className="flex items-center gap-1">
                        <MousePointer className="h-4 w-4" />
                        Clicks
                        {sortColumn === 'clicks' && (
                          sortDirection === 'desc' ? <ArrowDownIcon className="h-3 w-3" /> : <ArrowUpIcon className="h-3 w-3" />
                        )}
                      </div>
                    </TableHead>
                    <TableHead 
                      className="cursor-pointer hover:bg-muted/50"
                      onClick={() => handleSort('ctr')}
                    >
                      <div className="flex items-center gap-1">
                        CTR
                        {sortColumn === 'ctr' && (
                          sortDirection === 'desc' ? <ArrowDownIcon className="h-3 w-3" /> : <ArrowUpIcon className="h-3 w-3" />
                        )}
                      </div>
                    </TableHead>
                    <TableHead 
                      className="cursor-pointer hover:bg-muted/50"
                      onClick={() => handleSort('position')}
                    >
                      <div className="flex items-center gap-1">
                        <Target className="h-4 w-4" />
                        Position
                        {sortColumn === 'position' && (
                          sortDirection === 'desc' ? <ArrowDownIcon className="h-3 w-3" /> : <ArrowUpIcon className="h-3 w-3" />
                        )}
                      </div>
                    </TableHead>
                    <TableHead>Trend</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {sortedQueries.slice(0, 50).map((query, index) => (
                    <TableRow key={index}>
                      <TableCell className="font-medium">
                        <div className="max-w-md truncate" title={query.query}>
                          {query.query}
                        </div>
                      </TableCell>
                      <TableCell>{formatNumber(query.impressions)}</TableCell>
                      <TableCell>{formatNumber(query.clicks)}</TableCell>
                      <TableCell>
                        <span style={{ color: getCtrColor(query.ctr, query.position) }}>
                          {formatPercentage(query.ctr)}
                        </span>
                      </TableCell>
                      <TableCell>{query.position.toFixed(1)}</TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Badge variant={getTrendBadgeVariant(query.trend)} className="gap-1">
                            {getTrendIcon(query.trend)}
                            {query.trend === 'stable' ? 'Stable' : `${Math.abs(query.trend_pct).toFixed(1)}%`}
                          </Badge>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
            
            {sortedQueries.length > 50 && (
              <div className="text-sm text-muted-foreground text-center">
                Showing top 50 of {sortedQueries.length} queries
              </div>
            )}
          </TabsContent>

          {/* Distribution Histogram */}
          <TabsContent value="distribution" className="space-y-4">
            <div className="space-y-2">
              <h3 className="text-lg font-semibold">Query Performance Distribution</h3>
              <p className="text-sm text-muted-foreground">
                Distribution of impressions and clicks across query performance buckets
              </p>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Impressions Distribution */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Impressions by Query Bucket</CardTitle>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={data.query_distribution}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis 
                        dataKey="bucket" 
                        angle={-45}
                        textAnchor="end"
                        height={80}
                        fontSize={12}
                      />
                      <YAxis 
                        tickFormatter={(value) => formatNumber(value)}
                        fontSize={12}
                      />
                      <Tooltip 
                        formatter={(value: number) => formatNumber(value, 0)}
                        labelStyle={{ color: '#000' }}
                      />
                      <Legend />
                      <Bar dataKey="impressions" fill="#3b82f6" name="Impressions" />
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>

              {/* Clicks Distribution */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Clicks by Query Bucket</CardTitle>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={data.query_distribution}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis 
                        dataKey="bucket" 
                        angle={-45}
                        textAnchor="end"
                        height={80}
                        fontSize={12}
                      />
                      <YAxis 
                        tickFormatter={(value) => formatNumber(value)}
                        fontSize={12}
                      />
                      <Tooltip 
                        formatter={(value: number) => formatNumber(value, 0)}
                        labelStyle={{ color: '#000' }}
                      />
                      <Legend />
                      <Bar dataKey="clicks" fill="#10b981" name="Clicks" />
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            </div>

            {/* Query Count Distribution */}
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Number of Queries per Bucket</CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={data.query_distribution}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis 
                      dataKey="bucket" 
                      angle={-45}
                      textAnchor="end"
                      height={80}
                      fontSize={12}
                    />
                    <YAxis fontSize={12} />
                    <Tooltip labelStyle={{ color: '#000' }} />
                    <Legend />
                    <Bar dataKey="query_count" fill="#8b5cf6" name="Query Count" />
                  </BarChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          </TabsContent>

          {/* CTR Scatter Plot */}
          <TabsContent value="ctr" className="space-y-4">
            <div className="space-y-2">
              <h3 className="text-lg font-semibold">Click-Through Rate Analysis</h3>
              <p className="text-sm text-muted-foreground">
                CTR performance relative to position. Green indicates above-expected CTR, red indicates below-expected.
              </p>
            </div>

            <Card>
              <CardContent className="pt-6">
                <ResponsiveContainer width="100%" height={500}>
                  <ScatterChart
                    margin={{ top: 20, right: 20, bottom: 60, left: 20 }}
                  >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis 
                      type="number" 
                      dataKey="position" 
                      name="Position"
                      label={{ value: 'Average Position', position: 'bottom', offset: 40 }}
                      domain={[0, 'auto']}
                      reversed
                      fontSize={12}
                    />
                    <YAxis 
                      type="number" 
                      dataKey="ctr" 
                      name="CTR"
                      label={{ value: 'Click-Through Rate (%)', angle: -90, position: 'insideLeft' }}
                      tickFormatter={(value) => `${value.toFixed(1)}%`}
                      fontSize={12}
                    />
                    <Tooltip 
                      cursor={{ strokeDasharray: '3 3' }}
                      content={({ active, payload }) => {
                        if (active && payload && payload.length) {
                          const data = payload[0].payload;
                          return (
                            <div className="bg-white p-3 border rounded-lg shadow-lg">
                              <p className="font-semibold text-sm mb-2 max-w-xs truncate" title={data.query}>
                                {data.query}
                              </p>
                              <div className="space-y-1 text-xs">
                                <p>Position: {data.position.toFixed(1)}</p>
                                <p>CTR: {data.ctr.toFixed(2)}%</p>
                                <p>Clicks: {formatNumber(data.clicks)}</p>
                                <p>Impressions: {formatNumber(data.impressions)}</p>
                              </div>
                            </div>
                          );
                        }
                        return null;
                      }}
                    />
                    <Scatter 
                      data={scatterData} 
                      fill="#3b82f6"
                      fillOpacity={0.6}
                    >
                      {scatterData.map((entry, index) => (
                        <Cell 
                          key={`cell-${index}`} 
                          fill={getCtrColor(entry.ctr / 100, entry.position)}
                        />
                      ))}
                    </Scatter>
                  </ScatterChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>

            {/* CTR Insights */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium">High Performers</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold text-green-600">
                    {scatterData.filter(q => {
                      const expectedCtr: { [key: number]: number } = {
                        1: 30, 2: 15, 3: 10, 4: 7, 5: 5, 6: 4, 7: 3, 8: 2.5, 9: 2, 10: 1.5
                      };
                      const positionKey = Math.min(Math.round(q.position), 10);
                      const expected = expectedCtr[positionKey] || 1;
                      return q.ctr >= expected * 1.2;
                    }).length}
                  </div>
                  <p className="text-xs text-muted-foreground mt-1">
                    Queries with CTR ≥20% above expected
                  </p>
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium">Normal Range</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold text-gray-600">
                    {scatterData.filter(q => {
                      const expectedCtr: { [key: number]: number } = {
                        1: 30, 2: 15, 3: 10, 4: 7, 5: 5, 6: 4, 7: 3, 8: 2.5, 9: 2, 10: 1.5
                      };
                      const positionKey = Math.min(Math.round(q.position), 10);
                      const expected = expectedCtr[positionKey] || 1;
                      return q.ctr >= expected * 0.8 && q.ctr < expected * 1.2;
                    }).length}
                  </div>
                  <p className="text-xs text-muted-foreground mt-1">
                    Queries with CTR within ±20% of expected
                  </p>
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium">
                    <span className="flex items-center gap-1">
                      <AlertTriangle className="h-4 w-4" />
                      Underperformers
                    </span>
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold text-red-600">
                    {scatterData.filter(q => {
                      const expectedCtr: { [key: number]: number } = {
                        1: 30, 2: 15, 3: 10, 4: 7, 5: 5, 6: 4, 7: 3, 8: 2.5, 9: 2, 10: 1.5
                      };
                      const positionKey = Math.min(Math.round(q.position), 10);
                      const expected = expectedCtr[positionKey] || 1;
                      return q.ctr < expected * 0.8;
                    }).length}
                  </div>
                  <p className="text-xs text-muted-foreground mt-1">
                    Queries with CTR &lt;20% below expected
                  </p>
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          {/* Position Distribution */}
          <TabsContent value="positions" className="space-y-4">
            <div className="space-y-2">
              <h3 className="text-lg font-semibold">Position Distribution Analysis</h3>
              <p className="text-sm text-muted-foreground">
                How your queries are distributed across different position ranges and their performance
              </p>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Query Count by Position */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Queries by Position Range</CardTitle>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={data.position_distribution}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis 
                        dataKey="position_range"
                        fontSize={12}
                      />
                      <YAxis fontSize={12} />
                      <Tooltip labelStyle={{ color: '#000' }} />
                      <Legend />
                      <Bar dataKey="query_count" fill="#3b82f6" name="Query Count">
                        {data.position_distribution.map((entry, index) => (
                          <Cell key={`cell-${index}`} fill={positionColors[index % positionColors.length]} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>

              {/* Average CTR by Position */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Average CTR by Position Range</CardTitle>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={300}>
                    <LineChart data={data.position_distribution}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis 
                        dataKey="position_range"
                        fontSize={12}
                      />
                      <YAxis 
                        tickFormatter={(value) => `${(value * 100).toFixed(1)}%`}
                        fontSize={12}
                      />
                      <Tooltip 
                        formatter={(value: number) => `${(value * 100).toFixed(2)}%`}
                        labelStyle={{ color: '#000' }}
                      />
                      <Legend />
                      <Line 
                        type="monotone" 
                        dataKey="avg_ctr" 
                        stroke="#10b981" 
                        strokeWidth={2}
                        name="Average CTR"
                        dot={{ fill: '#10b981', r: 4 }}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            </div>

            {/* Clicks by Position */}
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Total Clicks by Position Range</CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={data.position_distribution}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis 
                      dataKey="position_range"
                      fontSize={12}
                    />
                    <YAxis 
                      tickFormatter={(value) => formatNumber(value)}
                      fontSize={12}
                    />
                    <Tooltip 
                      formatter={(value: number) => formatNumber(value, 0)}
                      labelStyle={{ color: '#000' }}
                    />
                    <Legend />
                    <Bar dataKey="total_clicks" fill="#ec4899" name="Total Clicks">
                      {data.position_distribution.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={positionColors[index % positionColors.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>

            {/* Position Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              {data.position_distribution.map((pos, index) => (
                <Card key={index}>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-sm font-medium">{pos.position_range}</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-2">
                    <div>
                      <div className="text-xs text-muted-foreground">Queries</div>
                      <div className="text-lg font-semibold">{pos.query_count}</div>
                    </div>
                    <div>
                      <div className="text-xs text-muted-foreground">Avg CTR</div>
                      <div className="text-lg font-semibold">{formatPercentage(pos.avg_ctr)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-muted-foreground">Total Clicks</div>
                      <div className="text-lg font-semibold">{formatNumber(pos.total_clicks)}</div>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          </TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  );
};

export default Module1QueryVisibility;
