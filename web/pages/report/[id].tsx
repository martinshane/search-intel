import React, { useEffect, useState } from 'react';
import { useRouter } from 'next/router';
import Head from 'next/head';
import {
  Container,
  Box,
  Typography,
  CircularProgress,
  Alert,
  Paper,
  Collapse,
  IconButton,
  LinearProgress,
  Chip,
} from '@mui/material';
import {
  ExpandMore as ExpandMoreIcon,
  TrendingUp,
  TrendingDown,
  RemoveCircleOutline,
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';
import {
  LineChart,
  Line,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
  Area,
  AreaChart,
} from 'recharts';

// API client
import { getReport } from '../../lib/api';

// Section components
import HealthSection from '../../components/report/HealthSection';
import PageTriageSection from '../../components/report/PageTriageSection';

// Types
interface Report {
  id: string;
  status: 'pending' | 'ingesting' | 'analyzing' | 'generating' | 'complete' | 'failed';
  progress: Record<string, string>;
  report_data?: ReportData;
  gsc_property: string;
  ga4_property?: string;
  created_at: string;
  completed_at?: string;
}

interface ReportData {
  health?: HealthData;
  page_triage?: PageTriageData;
  serp_landscape?: any;
  content_intelligence?: any;
  gameplan?: any;
  algorithm_impacts?: any;
  intent_migration?: any;
  ctr_modeling?: any;
  site_architecture?: any;
  branded_split?: any;
  competitive_threats?: any;
  revenue_attribution?: any;
}

interface HealthData {
  overall_direction: string;
  trend_slope_pct_per_month: number;
  change_points: ChangePoint[];
  seasonality: {
    best_day: string;
    worst_day: string;
    monthly_cycle: boolean;
    cycle_description: string;
  };
  anomalies: Anomaly[];
  forecast: {
    '30d': ForecastPoint;
    '60d': ForecastPoint;
    '90d': ForecastPoint;
  };
}

interface ChangePoint {
  date: string;
  magnitude: number;
  direction: string;
}

interface Anomaly {
  date: string;
  type: string;
  magnitude: number;
}

interface ForecastPoint {
  clicks: number;
  ci_low: number;
  ci_high: number;
}

interface PageTriageData {
  pages: PageTriageItem[];
  summary: {
    total_pages_analyzed: number;
    growing: number;
    stable: number;
    decaying: number;
    critical: number;
    total_recoverable_clicks_monthly: number;
  };
}

interface PageTriageItem {
  url: string;
  bucket: 'growing' | 'stable' | 'decaying' | 'critical';
  current_monthly_clicks: number;
  trend_slope: number;
  projected_page1_loss_date?: string;
  ctr_anomaly: boolean;
  ctr_expected?: number;
  ctr_actual?: number;
  engagement_flag?: string;
  priority_score: number;
  recommended_action: string;
}

// Styled components
const SectionCard = styled(Paper)(({ theme }) => ({
  marginBottom: theme.spacing(3),
  overflow: 'hidden',
}));

const SectionHeader = styled(Box)(({ theme }) => ({
  padding: theme.spacing(2, 3),
  background: theme.palette.mode === 'dark' ? theme.palette.grey[800] : theme.palette.grey[50],
  borderBottom: `1px solid ${theme.palette.divider}`,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  cursor: 'pointer',
  '&:hover': {
    background: theme.palette.mode === 'dark' ? theme.palette.grey[700] : theme.palette.grey[100],
  },
}));

const SectionContent = styled(Box)(({ theme }) => ({
  padding: theme.spacing(3),
}));

const TldrBox = styled(Box)(({ theme }) => ({
  padding: theme.spacing(2),
  marginBottom: theme.spacing(3),
  background: theme.palette.mode === 'dark' ? theme.palette.grey[900] : theme.palette.primary.light,
  borderRadius: theme.shape.borderRadius,
  borderLeft: `4px solid ${theme.palette.primary.main}`,
}));

const ExpandIcon = styled(ExpandMoreIcon, {
  shouldForwardProp: (prop) => prop !== 'expanded',
})<{ expanded: boolean }>(({ theme, expanded }) => ({
  transform: expanded ? 'rotate(180deg)' : 'rotate(0deg)',
  transition: theme.transitions.create('transform', {
    duration: theme.transitions.duration.shortest,
  }),
}));

export default function ReportPage() {
  const router = useRouter();
  const { id } = router.query;

  const [report, setReport] = useState<Report | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedSections, setExpandedSections] = useState<Set<string>>(
    new Set(['health', 'page_triage'])
  );

  useEffect(() => {
    if (!id || typeof id !== 'string') return;

    let pollInterval: NodeJS.Timeout;

    const fetchReport = async () => {
      try {
        const data = await getReport(id);
        setReport(data);

        // If report is still processing, continue polling
        if (['pending', 'ingesting', 'analyzing', 'generating'].includes(data.status)) {
          pollInterval = setTimeout(fetchReport, 3000);
        } else {
          setLoading(false);
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load report');
        setLoading(false);
      }
    };

    fetchReport();

    return () => {
      if (pollInterval) clearTimeout(pollInterval);
    };
  }, [id]);

  const toggleSection = (section: string) => {
    setExpandedSections((prev) => {
      const next = new Set(prev);
      if (next.has(section)) {
        next.delete(section);
      } else {
        next.add(section);
      }
      return next;
    });
  };

  const getProgressValue = () => {
    if (!report) return 0;
    if (report.status === 'complete') return 100;
    if (report.status === 'failed') return 0;

    const modules = [
      'health',
      'page_triage',
      'serp_landscape',
      'content_intelligence',
      'gameplan',
      'algorithm_impacts',
      'intent_migration',
      'ctr_modeling',
      'site_architecture',
      'branded_split',
      'competitive_threats',
      'revenue_attribution',
    ];

    const completed = modules.filter((m) => report.progress[m] === 'complete').length;
    return (completed / modules.length) * 100;
  };

  const getStatusLabel = () => {
    if (!report) return 'Loading...';
    switch (report.status) {
      case 'pending':
        return 'Queued for processing';
      case 'ingesting':
        return 'Fetching data from Google Search Console and Analytics';
      case 'analyzing':
        return 'Running analysis modules';
      case 'generating':
        return 'Generating report';
      case 'complete':
        return 'Report complete';
      case 'failed':
        return 'Report generation failed';
      default:
        return 'Processing';
    }
  };

  const getTrendIcon = (direction: string) => {
    if (direction.includes('growth')) return <TrendingUp color="success" />;
    if (direction.includes('decline')) return <TrendingDown color="error" />;
    return <RemoveCircleOutline color="action" />;
  };

  if (error) {
    return (
      <Container maxWidth="lg" sx={{ py: 4 }}>
        <Alert severity="error">{error}</Alert>
      </Container>
    );
  }

  if (loading || !report) {
    return (
      <Container maxWidth="lg" sx={{ py: 4 }}>
        <Box display="flex" flexDirection="column" alignItems="center" gap={3}>
          <CircularProgress />
          <Typography variant="h6">Loading report...</Typography>
        </Box>
      </Container>
    );
  }

  if (report.status !== 'complete' || !report.report_data) {
    return (
      <Container maxWidth="lg" sx={{ py: 4 }}>
        <Head>
          <title>Generating Report | Search Intelligence</title>
        </Head>
        <Paper sx={{ p: 4 }}>
          <Box display="flex" alignItems="center" gap={2} mb={3}>
            <CircularProgress size={24} />
            <Typography variant="h5">{getStatusLabel()}</Typography>
          </Box>
          <LinearProgress variant="determinate" value={getProgressValue()} sx={{ mb: 2 }} />
          <Typography variant="body2" color="text.secondary">
            {Math.round(getProgressValue())}% complete
          </Typography>
          {report.status === 'failed' && (
            <Alert severity="error" sx={{ mt: 3 }}>
              Report generation failed. Please try again or contact support.
            </Alert>
          )}
        </Paper>
      </Container>
    );
  }

  const { report_data } = report;

  return (
    <Container maxWidth="lg" sx={{ py: 4 }}>
      <Head>
        <title>Search Intelligence Report | {report.gsc_property}</title>
      </Head>

      {/* Report Header */}
      <Box mb={4}>
        <Typography variant="h3" gutterBottom>
          Search Intelligence Report
        </Typography>
        <Typography variant="h6" color="text.secondary" gutterBottom>
          {report.gsc_property}
        </Typography>
        <Box display="flex" gap={2} alignItems="center">
          <Chip
            label={`Generated ${new Date(report.completed_at!).toLocaleDateString()}`}
            size="small"
          />
          {report.ga4_property && (
            <Chip label={`GA4: ${report.ga4_property}`} size="small" variant="outlined" />
          )}
        </Box>
      </Box>

      {/* Section 1: Health & Trajectory */}
      {report_data.health && (
        <SectionCard elevation={2}>
          <SectionHeader onClick={() => toggleSection('health')}>
            <Box display="flex" alignItems="center" gap={2}>
              {getTrendIcon(report_data.health.overall_direction)}
              <Box>
                <Typography variant="h6">Health & Trajectory</Typography>
                <Typography variant="body2" color="text.secondary">
                  {report_data.health.overall_direction.replace(/_/g, ' ')} at{' '}
                  {Math.abs(report_data.health.trend_slope_pct_per_month).toFixed(1)}% per month
                </Typography>
              </Box>
            </Box>
            <IconButton>
              <ExpandIcon expanded={expandedSections.has('health')} />
            </IconButton>
          </SectionHeader>
          <Collapse in={expandedSections.has('health')} timeout="auto" unmountOnExit>
            <SectionContent>
              <HealthSection data={report_data.health} />
            </SectionContent>
          </Collapse>
        </SectionCard>
      )}

      {/* Section 2: Page-Level Triage */}
      {report_data.page_triage && (
        <SectionCard elevation={2}>
          <SectionHeader onClick={() => toggleSection('page_triage')}>
            <Box display="flex" alignItems="center" gap={2}>
              <Box>
                <Typography variant="h6">Page Triage</Typography>
                <Typography variant="body2" color="text.secondary">
                  {report_data.page_triage.summary.critical} critical,{' '}
                  {report_data.page_triage.summary.decaying} decaying —{' '}
                  {report_data.page_triage.summary.total_recoverable_clicks_monthly.toLocaleString()}{' '}
                  clicks/month recoverable
                </Typography>
              </Box>
            </Box>
            <IconButton>
              <ExpandIcon expanded={expandedSections.has('page_triage')} />
            </IconButton>
          </SectionHeader>
          <Collapse in={expandedSections.has('page_triage')} timeout="auto" unmountOnExit>
            <SectionContent>
              <PageTriageSection data={report_data.page_triage} />
            </SectionContent>
          </Collapse>
        </SectionCard>
      )}

      {/* Section 3: SERP Landscape (placeholder) */}
      {report_data.serp_landscape && (
        <SectionCard elevation={2}>
          <SectionHeader onClick={() => toggleSection('serp')}>
            <Box>
              <Typography variant="h6">SERP Landscape Analysis</Typography>
              <Typography variant="body2" color="text.secondary">
                Competitor mapping and SERP feature opportunities
              </Typography>
            </Box>
            <IconButton>
              <ExpandIcon expanded={expandedSections.has('serp')} />
            </IconButton>
          </SectionHeader>
          <Collapse in={expandedSections.has('serp')} timeout="auto" unmountOnExit>
            <SectionContent>
              <Typography>SERP analysis coming soon...</Typography>
            </SectionContent>
          </Collapse>
        </SectionCard>
      )}

      {/* Section 4: Content Intelligence (placeholder) */}
      {report_data.content_intelligence && (
        <SectionCard elevation={2}>
          <SectionHeader onClick={() => toggleSection('content')}>
            <Box>
              <Typography variant="h6">Content Intelligence</Typography>
              <Typography variant="body2" color="text.secondary">
                Cannibalization, striking distance, and content gaps
              </Typography>
            </Box>
            <IconButton>
              <ExpandIcon expanded={expandedSections.has('content')} />
            </IconButton>
          </SectionHeader>
          <Collapse in={expandedSections.has('content')} timeout="auto" unmountOnExit>
            <SectionContent>
              <Typography>Content analysis coming soon...</Typography>
            </SectionContent>
          </Collapse>
        </SectionCard>
      )}

      {/* Section 5: The Gameplan (placeholder) */}
      {report_data.gameplan && (
        <SectionCard elevation={2}>
          <SectionHeader onClick={() => toggleSection('gameplan')}>
            <Box>
              <Typography variant="h6">The Gameplan</Typography>
              <Typography variant="body2" color="text.secondary">
                Prioritized action plan with impact estimates
              </Typography>
            </Box>
            <IconButton>
              <ExpandIcon expanded={expandedSections.has('gameplan')} />
            </IconButton>
          </SectionHeader>
          <Collapse in={expandedSections.has('gameplan')} timeout="auto" unmountOnExit>
            <SectionContent>
              <Typography>Action plan coming soon...</Typography>
            </SectionContent>
          </Collapse>
        </SectionCard>
      )}

      {/* Consulting CTA */}
      <Paper sx={{ p: 4, mt: 4, textAlign: 'center', background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)', color: 'white' }}>
        <Typography variant="h5" gutterBottom>
          Want help executing this plan?
        </Typography>
        <Typography variant="body1" sx={{ mb: 3 }}>
          Our search intelligence consulting team can implement these recommendations and drive measurable traffic growth.
        </Typography>
        <Box component="a" href="/contact" sx={{ 
          display: 'inline-block',
          px: 4,
          py: 1.5,
          background: 'white',
          color: '#667eea',
          borderRadius: 2,
          textDecoration: 'none',
          fontWeight: 600,
          '&:hover': {
            background: '#f0f0f0',
          }
        }}>
          Book a Call
        </Box>
      </Paper>
    </Container>
  );
}
