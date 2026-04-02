import React, { useEffect, useState } from 'react';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Card, CardContent } from '@/components/ui/card';
import { Loader2 } from 'lucide-react';
import Module1TrafficOverview from './modules/Module1TrafficOverview';

interface ModuleRendererProps {
  reportId: string;
  moduleNumber: number;
  siteUrl?: string;
}

interface ModuleData {
  module_number: number;
  title: string;
  data: any;
  generated_at: string;
  status: 'completed' | 'processing' | 'failed' | 'pending';
  error_message?: string;
}

interface ModuleComponentProps {
  data: any;
  reportId: string;
  siteUrl?: string;
}

// Module component mapping - Add new modules here as they're implemented
const MODULE_COMPONENTS: Record<number, React.ComponentType<ModuleComponentProps>> = {
  1: Module1TrafficOverview,
  // 2: Module2PageTriage,
  // 3: Module3SerpLandscape,
  // 4: Module4ContentIntelligence,
  // 5: Module5Gameplan,
  // 6: Module6AlgorithmImpact,
  // 7: Module7IntentMigration,
  // 8: Module8SeasonalPatterns,
  // 9: Module9InternalLinkAnalysis,
  // 10: Module10TechnicalHealth,
  // 11: Module11CompetitiveLandscape,
  // 12: Module12ExecutiveSummary,
};

const MODULE_TITLES: Record<number, string> = {
  1: 'Traffic Health & Trajectory',
  2: 'Page-Level Triage',
  3: 'SERP Landscape Analysis',
  4: 'Content Intelligence',
  5: 'The Gameplan',
  6: 'Algorithm Update Impact',
  7: 'Query Intent Migration',
  8: 'Seasonal Patterns & Forecasting',
  9: 'Internal Link Analysis',
  10: 'Technical Health Signals',
  11: 'Competitive Landscape',
  12: 'Executive Summary',
};

const ModuleRenderer: React.FC<ModuleRendererProps> = ({ reportId, moduleNumber, siteUrl }) => {
  const [moduleData, setModuleData] = useState<ModuleData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [retryCount, setRetryCount] = useState(0);

  const maxRetries = 3;
  const retryDelay = 2000; // 2 seconds

  useEffect(() => {
    let mounted = true;
    let retryTimeout: NodeJS.Timeout;

    const fetchModuleData = async () => {
      try {
        setLoading(true);
        setError(null);

        const response = await fetch(`/api/reports/${reportId}/modules/${moduleNumber}`);
        
        if (!response.ok) {
          if (response.status === 404) {
            throw new Error(`Module ${moduleNumber} data not found`);
          }
          if (response.status === 500) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Server error loading module data');
          }
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const data: ModuleData = await response.json();
        
        if (!mounted) return;

        // Handle different module statuses
        if (data.status === 'processing' || data.status === 'pending') {
          // If still processing, retry after delay
          if (retryCount < maxRetries) {
            retryTimeout = setTimeout(() => {
              if (mounted) {
                setRetryCount(prev => prev + 1);
              }
            }, retryDelay);
          } else {
            setError('Module is still processing. Please refresh the page in a few moments.');
            setLoading(false);
          }
          return;
        }

        if (data.status === 'failed') {
          throw new Error(data.error_message || 'Module generation failed');
        }

        // Successfully loaded completed module
        setModuleData(data);
        setLoading(false);

      } catch (err) {
        if (!mounted) return;
        
        const errorMessage = err instanceof Error ? err.message : 'Failed to load module data';
        setError(errorMessage);
        setLoading(false);

        // Log error for debugging
        console.error(`Error loading module ${moduleNumber} for report ${reportId}:`, err);
      }
    };

    fetchModuleData();

    return () => {
      mounted = false;
      if (retryTimeout) {
        clearTimeout(retryTimeout);
      }
    };
  }, [reportId, moduleNumber, retryCount]);

  // Error boundary fallback
  if (error) {
    return (
      <Card className="border-red-200 bg-red-50">
        <CardContent className="pt-6">
          <Alert variant="destructive">
            <AlertDescription>
              <div className="font-semibold mb-2">Error Loading Module {moduleNumber}</div>
              <div className="text-sm">{error}</div>
              {retryCount >= maxRetries && (
                <button
                  onClick={() => {
                    setRetryCount(0);
                    setError(null);
                  }}
                  className="mt-3 text-sm underline hover:no-underline"
                >
                  Try again
                </button>
              )}
            </AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    );
  }

  // Loading state
  if (loading) {
    return (
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-col items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin text-blue-600 mb-4" />
            <p className="text-sm text-gray-600">
              Loading {MODULE_TITLES[moduleNumber] || `Module ${moduleNumber}`}...
            </p>
            {retryCount > 0 && (
              <p className="text-xs text-gray-500 mt-2">
                Module is processing (attempt {retryCount + 1}/{maxRetries + 1})
              </p>
            )}
          </div>
        </CardContent>
      </Card>
    );
  }

  // Module not yet implemented
  const ModuleComponent = MODULE_COMPONENTS[moduleNumber];
  if (!ModuleComponent) {
    return (
      <Card className="border-gray-200 bg-gray-50">
        <CardContent className="pt-6">
          <Alert>
            <AlertDescription>
              <div className="font-semibold mb-2">
                {MODULE_TITLES[moduleNumber] || `Module ${moduleNumber}`}
              </div>
              <div className="text-sm text-gray-600">
                This module visualization is coming soon. Data has been generated and will be displayed once the component is implemented.
              </div>
              {moduleData && (
                <details className="mt-3">
                  <summary className="text-xs text-gray-500 cursor-pointer hover:text-gray-700">
                    View raw data
                  </summary>
                  <pre className="mt-2 p-3 bg-white rounded border text-xs overflow-auto max-h-96">
                    {JSON.stringify(moduleData.data, null, 2)}
                  </pre>
                </details>
              )}
            </AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    );
  }

  // No data available
  if (!moduleData || !moduleData.data) {
    return (
      <Card className="border-yellow-200 bg-yellow-50">
        <CardContent className="pt-6">
          <Alert>
            <AlertDescription>
              <div className="font-semibold mb-2">No Data Available</div>
              <div className="text-sm">
                Module {moduleNumber} data is not yet available for this report. 
                The report may still be generating, or there may have been an issue collecting data for this section.
              </div>
            </AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    );
  }

  // Render the module component with error boundary
  return (
    <ErrorBoundary moduleNumber={moduleNumber} moduleName={MODULE_TITLES[moduleNumber]}>
      <ModuleComponent 
        data={moduleData.data} 
        reportId={reportId}
        siteUrl={siteUrl}
      />
    </ErrorBoundary>
  );
};

// Error boundary component for catching runtime errors in module components
class ErrorBoundary extends React.Component<
  { children: React.ReactNode; moduleNumber: number; moduleName?: string },
  { hasError: boolean; error: Error | null }
> {
  constructor(props: any) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error(
      `Error in Module ${this.props.moduleNumber} (${this.props.moduleName}):`,
      error,
      errorInfo
    );
  }

  render() {
    if (this.state.hasError) {
      return (
        <Card className="border-red-200 bg-red-50">
          <CardContent className="pt-6">
            <Alert variant="destructive">
              <AlertDescription>
                <div className="font-semibold mb-2">
                  Error Rendering {this.props.moduleName || `Module ${this.props.moduleNumber}`}
                </div>
                <div className="text-sm mb-2">
                  An unexpected error occurred while displaying this module.
                </div>
                {this.state.error && (
                  <details className="text-xs">
                    <summary className="cursor-pointer hover:underline">
                      Technical details
                    </summary>
                    <pre className="mt-2 p-2 bg-white rounded border overflow-auto">
                      {this.state.error.message}
                      {'\n\n'}
                      {this.state.error.stack}
                    </pre>
                  </details>
                )}
                <button
                  onClick={() => window.location.reload()}
                  className="mt-3 text-sm underline hover:no-underline"
                >
                  Reload page
                </button>
              </AlertDescription>
            </Alert>
          </CardContent>
        </Card>
      );
    }

    return this.props.children;
  }
}

export default ModuleRenderer;
