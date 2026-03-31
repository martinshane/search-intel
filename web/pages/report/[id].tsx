import React, { useEffect, useState } from 'react';
import { useRouter } from 'next/router';
import Head from 'next/head';
import { Box, Container, Typography, CircularProgress, Alert, Paper } from '@mui/material';
import HealthTrajectoryChart from '../../components/HealthTrajectoryChart';

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
    "30d": { clicks: number; ci_low: number; ci_high: number };
    "60d": { clicks: number; ci_low: number; ci_high: number };
    "90d": { clicks: number; ci_low: number; ci_high: number };
  };
}

interface DailyDataPoint {
  date: string;
  clicks: number;
  impressions: number;
  ctr: number;
  position: number;
  trend?: number;
  forecast?: number;
  ci_low?: number;
  ci_high?: number;
}

interface Report {
  id: string;
  status: string;
  gsc_property: string;
  ga4_property: string | null;
  created_at: string;
  completed_at: string | null;
  report_data: {
    module_1?: Module1Data;
    daily_data?: DailyDataPoint[];
  } | null;
}

export default function ReportPage() {
  const router = useRouter();
  const { id } = router.query;
  const [report, setReport] = useState<Report | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;

    const fetchReport = async () => {
      try {
        setLoading(true);
        setError(null);

        const response = await fetch(`/api/reports/${id}`);
        
        if (!response.ok) {
          if (response.status === 404) {
            throw new Error('Report not found');
          }
          throw new Error(`Failed to fetch report: ${response.statusText}`);
        }

        const data = await response.json();
        setReport(data);

        // Poll for updates if report is still processing
        if (data.status === 'pending' || data.status === 'ingesting' || data.status === 'analyzing' || data.status === 'generating') {
          setTimeout(fetchReport, 5000); // Poll every 5 seconds
        }
      } catch (err) {
        console.error('Error fetching report:', err);
        setError(err instanceof Error ? err.message : 'An unknown error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchReport();
  }, [id]);

  if (loading && !report) {
    return (
      <Container maxWidth="lg" sx={{ mt: 4, mb: 4, display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '60vh' }}>
        <Box sx={{ textAlign: 'center' }}>
          <CircularProgress size={60} />
          <Typography variant="h6" sx={{ mt: 2 }}>
            Loading report...
          </Typography>
        </Box>
      </Container>
    );
  }

  if (error) {
    return (
      <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      </Container>
    );
  }

  if (!report) {
    return (
      <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
        <Alert severity="info">
          No report data available
        </Alert>
      </Container>
    );
  }

  const isProcessing = ['pending', 'ingesting', 'analyzing', 'generating'].includes(report.status);
  const module1Data = report.report_data?.module_1;
  const dailyData = report.report_data?.daily_data;

  return (
    <>
      <Head>
        <title>Search Intelligence Report - {report.gsc_property}</title>
        <meta name="description" content={`Comprehensive search intelligence report for ${report.gsc_property}`} />
      </Head>

      <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
        <Box sx={{ mb: 4 }}>
          <Typography variant="h3" component="h1" gutterBottom>
            Search Intelligence Report
          </Typography>
          <Typography variant="subtitle1" color="text.secondary" gutterBottom>
            {report.gsc_property}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Generated: {new Date(report.created_at).toLocaleDateString()}
            {report.completed_at && ` • Completed: ${new Date(report.completed_at).toLocaleDateString()}`}
          </Typography>
        </Box>

        {isProcessing && (
          <Alert severity="info" sx={{ mb: 3 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <CircularProgress size={20} />
              <Box>
                <Typography variant="body1" fontWeight="bold">
                  Report Generation In Progress
                </Typography>
                <Typography variant="body2">
                  Status: {report.status}
                  {report.status === 'analyzing' && ' - Running analysis modules...'}
                  {report.status === 'ingesting' && ' - Fetching data from Google Search Console and Analytics...'}
                  {report.status === 'generating' && ' - Generating final report...'}
                </Typography>
              </Box>
            </Box>
          </Alert>
        )}

        {report.status === 'failed' && (
          <Alert severity="error" sx={{ mb: 3 }}>
            Report generation failed. Please try again or contact support.
          </Alert>
        )}

        {module1Data && dailyData && (
          <Paper elevation={2} sx={{ p: 3, mb: 3 }}>
            <Typography variant="h5" gutterBottom>
              Health & Trajectory Analysis
            </Typography>
            <Typography variant="body2" color="text.secondary" gutterBottom sx={{ mb: 3 }}>
              Your site is currently <strong>{module1Data.overall_direction}</strong> at{' '}
              <strong>{module1Data.trend_slope_pct_per_month > 0 ? '+' : ''}{module1Data.trend_slope_pct_per_month.toFixed(1)}%</strong>{' '}
              per month
            </Typography>

            <HealthTrajectoryChart
              data={dailyData}
              changePoints={module1Data.change_points}
              forecast={module1Data.forecast}
            />

            <Box sx={{ mt: 3 }}>
              <Typography variant="h6" gutterBottom>
                Key Insights
              </Typography>
              
              {module1Data.change_points.length > 0 && (
                <Box sx={{ mb: 2 }}>
                  <Typography variant="subtitle2" gutterBottom>
                    Change Points Detected:
                  </Typography>
                  {module1Data.change_points.map((cp, idx) => (
                    <Typography key={idx} variant="body2" sx={{ ml: 2 }}>
                      • {new Date(cp.date).toLocaleDateString()}: {cp.direction} ({(cp.magnitude * 100).toFixed(1)}%)
                    </Typography>
                  ))}
                </Box>
              )}

              <Box sx={{ mb: 2 }}>
                <Typography variant="subtitle2" gutterBottom>
                  Seasonality:
                </Typography>
                <Typography variant="body2" sx={{ ml: 2 }}>
                  • Best performing day: {module1Data.seasonality.best_day}
                </Typography>
                <Typography variant="body2" sx={{ ml: 2 }}>
                  • Worst performing day: {module1Data.seasonality.worst_day}
                </Typography>
                {module1Data.seasonality.monthly_cycle && (
                  <Typography variant="body2" sx={{ ml: 2 }}>
                    • Monthly pattern: {module1Data.seasonality.cycle_description}
                  </Typography>
                )}
              </Box>

              {module1Data.anomalies.length > 0 && (
                <Box sx={{ mb: 2 }}>
                  <Typography variant="subtitle2" gutterBottom>
                    Anomalies Detected:
                  </Typography>
                  {module1Data.anomalies.slice(0, 5).map((anomaly, idx) => (
                    <Typography key={idx} variant="body2" sx={{ ml: 2 }}>
                      • {new Date(anomaly.date).toLocaleDateString()}: {anomaly.type} ({(anomaly.magnitude * 100).toFixed(1)}%)
                    </Typography>
                  ))}
                </Box>
              )}

              <Box>
                <Typography variant="subtitle2" gutterBottom>
                  Forecast:
                </Typography>
                <Typography variant="body2" sx={{ ml: 2 }}>
                  • 30 days: {module1Data.forecast["30d"].clicks.toLocaleString()} clicks 
                  ({module1Data.forecast["30d"].ci_low.toLocaleString()} - {module1Data.forecast["30d"].ci_high.toLocaleString()})
                </Typography>
                <Typography variant="body2" sx={{ ml: 2 }}>
                  • 60 days: {module1Data.forecast["60d"].clicks.toLocaleString()} clicks 
                  ({module1Data.forecast["60d"].ci_low.toLocaleString()} - {module1Data.forecast["60d"].ci_high.toLocaleString()})
                </Typography>
                <Typography variant="body2" sx={{ ml: 2 }}>
                  • 90 days: {module1Data.forecast["90d"].clicks.toLocaleString()} clicks 
                  ({module1Data.forecast["90d"].ci_low.toLocaleString()} - {module1Data.forecast["90d"].ci_high.toLocaleString()})
                </Typography>
              </Box>
            </Box>
          </Paper>
        )}

        {report.status === 'complete' && !module1Data && (
          <Alert severity="warning" sx={{ mb: 3 }}>
            Report completed but no analysis data available. This may indicate an issue with data processing.
          </Alert>
        )}

        {!isProcessing && !module1Data && report.status !== 'failed' && (
          <Paper elevation={1} sx={{ p: 4, textAlign: 'center' }}>
            <Typography variant="h6" color="text.secondary">
              Additional report sections coming soon...
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              This report is being actively generated. Check back in a few minutes.
            </Typography>
          </Paper>
        )}
      </Container>
    </>
  );
}