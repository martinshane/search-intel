import { useEffect, useState } from 'react';
import { useRouter } from 'next/router';
import Head from 'next/head';
import Link from 'next/link';
import styles from '../../styles/Report.module.css';

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
}

interface ReportMetadata {
  report_id: string;
  domain: string;
  created_at: string;
  status: string;
  user_id: string;
}

export default function ReportPage() {
  const router = useRouter();
  const { reportId } = router.query;
  
  const [metadata, setMetadata] = useState<ReportMetadata | null>(null);
  const [module1Data, setModule1Data] = useState<Module1Data | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [metadataError, setMetadataError] = useState<string | null>(null);
  const [module1Error, setModule1Error] = useState<string | null>(null);

  useEffect(() => {
    if (!reportId) return;

    const fetchReportData = async () => {
      setLoading(true);
      setError(null);
      setMetadataError(null);
      setModule1Error(null);

      try {
        // Fetch report metadata
        const metadataResponse = await fetch(`/api/reports/${reportId}`);
        if (!metadataResponse.ok) {
          const errorData = await metadataResponse.json().catch(() => ({}));
          throw new Error(errorData.error || `Failed to fetch report metadata: ${metadataResponse.status}`);
        }
        const metadataJson = await metadataResponse.json();
        setMetadata(metadataJson);

        // Fetch Module 1 data
        try {
          const module1Response = await fetch(`/api/reports/${reportId}/modules/1`);
          if (!module1Response.ok) {
            const errorData = await module1Response.json().catch(() => ({}));
            setModule1Error(errorData.error || `Failed to fetch module data: ${module1Response.status}`);
          } else {
            const module1Json = await module1Response.json();
            setModule1Data(module1Json);
          }
        } catch (err) {
          const errorMessage = err instanceof Error ? err.message : 'Unknown error occurred';
          setModule1Error(`Error loading Module 1: ${errorMessage}`);
        }

      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Unknown error occurred';
        setError(errorMessage);
        setMetadataError(errorMessage);
      } finally {
        setLoading(false);
      }
    };

    fetchReportData();
  }, [reportId]);

  if (loading) {
    return (
      <div className={styles.container}>
        <Head>
          <title>Loading Report... | Search Intelligence Report</title>
        </Head>
        <div className={styles.loading}>
          <div className={styles.spinner}></div>
          <p>Loading your report...</p>
        </div>
      </div>
    );
  }

  if (error || metadataError) {
    return (
      <div className={styles.container}>
        <Head>
          <title>Error | Search Intelligence Report</title>
        </Head>
        <div className={styles.error}>
          <h1>Error Loading Report</h1>
          <p>{error || metadataError}</p>
          <Link href="/">
            <a className={styles.backButton}>← Back to Home</a>
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className={styles.container}>
      <Head>
        <title>
          {metadata?.domain ? `${metadata.domain} - Report` : 'Report'} | Search Intelligence Report
        </title>
        <meta name="description" content="Comprehensive search intelligence analysis" />
      </Head>

      <header className={styles.header}>
        <div className={styles.headerContent}>
          <Link href="/">
            <a className={styles.backLink}>← Back to Dashboard</a>
          </Link>
          <h1 className={styles.title}>Search Intelligence Report</h1>
          {metadata && (
            <div className={styles.reportMeta}>
              <h2 className={styles.domain}>{metadata.domain}</h2>
              <p className={styles.date}>
                Generated: {new Date(metadata.created_at).toLocaleDateString('en-US', {
                  year: 'numeric',
                  month: 'long',
                  day: 'numeric',
                  hour: '2-digit',
                  minute: '2-digit'
                })}
              </p>
              <span className={`${styles.status} ${styles[metadata.status]}`}>
                {metadata.status}
              </span>
            </div>
          )}
        </div>
      </header>

      <main className={styles.main}>
        <nav className={styles.moduleNav}>
          <h3>Report Sections</h3>
          <ul>
            <li className={styles.active}>
              <a href="#module1">1. Health & Trajectory</a>
            </li>
            <li className={styles.disabled}>2. Page-Level Triage</li>
            <li className={styles.disabled}>3. SERP Landscape</li>
            <li className={styles.disabled}>4. Content Intelligence</li>
            <li className={styles.disabled}>5. The Gameplan</li>
            <li className={styles.disabled}>6. Algorithm Impact</li>
            <li className={styles.disabled}>7. Intent Migration</li>
            <li className={styles.disabled}>8. Crawl & Technical</li>
            <li className={styles.disabled}>9. Internal Link Analysis</li>
            <li className={styles.disabled}>10. Seasonality Forecast</li>
            <li className={styles.disabled}>11. Traffic Attribution</li>
            <li className={styles.disabled}>12. Competitive Benchmarking</li>
          </ul>
        </nav>

        <div className={styles.content}>
          {/* Module 1: Health & Trajectory */}
          <section id="module1" className={styles.module}>
            <div className={styles.moduleHeader}>
              <h2>Module 1: Health & Trajectory</h2>
              <p className={styles.moduleDescription}>
                Statistical analysis of traffic trends, seasonality patterns, and forecasting
              </p>
            </div>

            {module1Error ? (
              <div className={styles.moduleError}>
                <p>⚠️ {module1Error}</p>
                <p className={styles.errorDetail}>
                  This module may still be processing or encountered an error during generation.
                </p>
              </div>
            ) : module1Data ? (
              <Module1Overview data={module1Data} />
            ) : (
              <div className={styles.moduleEmpty}>
                <p>No data available for this module yet.</p>
              </div>
            )}
          </section>

          {/* Placeholder sections for future modules */}
          <section className={styles.module}>
            <div className={styles.moduleHeader}>
              <h2>Module 2: Page-Level Triage</h2>
              <p className={styles.moduleDescription}>Coming soon...</p>
            </div>
            <div className={styles.modulePlaceholder}>
              <p>This module will identify pages that need immediate attention based on performance trends.</p>
            </div>
          </section>

          <section className={styles.module}>
            <div className={styles.moduleHeader}>
              <h2>Module 3: SERP Landscape Analysis</h2>
              <p className={styles.moduleDescription}>Coming soon...</p>
            </div>
            <div className={styles.modulePlaceholder}>
              <p>This module will analyze competitive SERP positioning and feature displacement.</p>
            </div>
          </section>
        </div>
      </main>
    </div>
  );
}

function Module1Overview({ data }: { data: Module1Data }) {
  return (
    <div className={styles.module1Container}>
      {/* Overall Direction Card */}
      <div className={styles.card}>
        <h3 className={styles.cardTitle}>Overall Traffic Direction</h3>
        <div className={styles.directionCard}>
          <div className={`${styles.directionBadge} ${styles[data.overall_direction.replace('_', '')]}`}>
            {data.overall_direction.replace('_', ' ').toUpperCase()}
          </div>
          <div className={styles.trendMetric}>
            <span className={styles.metricValue}>
              {data.trend_slope_pct_per_month > 0 ? '+' : ''}
              {data.trend_slope_pct_per_month.toFixed(1)}%
            </span>
            <span className={styles.metricLabel}>per month</span>
          </div>
        </div>
      </div>

      {/* Forecast Card */}
      <div className={styles.card}>
        <h3 className={styles.cardTitle}>Traffic Forecast</h3>
        <div className={styles.forecastGrid}>
          <ForecastPeriod 
            label="30 Days"
            data={data.forecast['30d']}
          />
          <ForecastPeriod 
            label="60 Days"
            data={data.forecast['60d']}
          />
          <ForecastPeriod 
            label="90 Days"
            data={data.forecast['90d']}
          />
        </div>
      </div>

      {/* Seasonality Card */}
      <div className={styles.card}>
        <h3 className={styles.cardTitle}>Seasonality Patterns</h3>
        <div className={styles.seasonalityContent}>
          <div className={styles.seasonalityRow}>
            <span className={styles.label}>Best Day:</span>
            <span className={styles.value}>{data.seasonality.best_day}</span>
          </div>
          <div className={styles.seasonalityRow}>
            <span className={styles.label}>Worst Day:</span>
            <span className={styles.value}>{data.seasonality.worst_day}</span>
          </div>
          <div className={styles.seasonalityRow}>
            <span className={styles.label}>Monthly Cycle:</span>
            <span className={styles.value}>
              {data.seasonality.monthly_cycle ? 'Yes' : 'No'}
            </span>
          </div>
          {data.seasonality.cycle_description && (
            <div className={styles.cycleDescription}>
              <p>{data.seasonality.cycle_description}</p>
            </div>
          )}
        </div>
      </div>

      {/* Change Points Card */}
      {data.change_points && data.change_points.length > 0 && (
        <div className={styles.card}>
          <h3 className={styles.cardTitle}>Significant Changes Detected</h3>
          <div className={styles.changePointsList}>
            {data.change_points.map((cp, index) => (
              <div key={index} className={styles.changePoint}>
                <div className={styles.changePointDate}>
                  {new Date(cp.date).toLocaleDateString('en-US', {
                    year: 'numeric',
                    month: 'short',
                    day: 'numeric'
                  })}
                </div>
                <div className={`${styles.changePointBadge} ${styles[cp.direction]}`}>
                  {cp.direction}
                </div>
                <div className={styles.changePointMagnitude}>
                  {cp.magnitude > 0 ? '+' : ''}
                  {(cp.magnitude * 100).toFixed(1)}%
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Anomalies Card */}
      {data.anomalies && data.anomalies.length > 0 && (
        <div className={styles.card}>
          <h3 className={styles.cardTitle}>Anomalies Detected</h3>
          <div className={styles.anomaliesList}>
            {data.anomalies.map((anomaly, index) => (
              <div key={index} className={styles.anomaly}>
                <div className={styles.anomalyDate}>
                  {new Date(anomaly.date).toLocaleDateString('en-US', {
                    year: 'numeric',
                    month: 'short',
                    day: 'numeric'
                  })}
                </div>
                <div className={styles.anomalyType}>
                  {anomaly.type}
                </div>
                <div className={styles.anomalyMagnitude}>
                  Impact: {anomaly.magnitude > 0 ? '+' : ''}
                  {(anomaly.magnitude * 100).toFixed(1)}%
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function ForecastPeriod({ 
  label, 
  data 
}: { 
  label: string; 
  data: { clicks: number; ci_low: number; ci_high: number } 
}) {
  const formatNumber = (num: number) => {
    return Math.round(num).toLocaleString();
  };

  const range = data.ci_high - data.ci_low;
  const uncertainty = (range / data.clicks * 100).toFixed(0);

  return (
    <div className={styles.forecastPeriod}>
      <div className={styles.forecastLabel}>{label}</div>
      <div className={styles.forecastValue}>{formatNumber(data.clicks)}</div>
      <div className={styles.forecastRange}>
        {formatNumber(data.ci_low)} - {formatNumber(data.ci_high)}
      </div>
      <div className={styles.forecastUncertainty}>
        ±{uncertainty}% uncertainty
      </div>
    </div>
  );
}
