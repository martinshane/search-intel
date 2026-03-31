import { useEffect, useState } from 'react';
import Link from 'next/link';
import { CheckCircle, Clock, AlertCircle, Loader2, ChevronDown, ChevronUp, ExternalLink } from 'lucide-react';

interface ProgressStep {
  module: string;
  status: 'pending' | 'running' | 'complete' | 'failed';
  startedAt?: string;
  completedAt?: string;
  error?: string;
  duration?: number;
}

interface Report {
  id: string;
  gscProperty: string;
  ga4Property: string;
  status: string;
  progress: Record<string, string>;
  createdAt: string;
  completedAt?: string;
}

const MODULE_NAMES: Record<string, string> = {
  'module_1': 'Health & Trajectory Analysis',
  'module_2': 'Page-Level Triage',
  'module_3': 'SERP Landscape Analysis',
  'module_4': 'Content Intelligence',
  'module_5': 'Strategic Gameplan',
  'module_6': 'Algorithm Update Impact',
  'module_7': 'Query Intent Migration',
  'module_8': 'CTR Modeling',
  'module_9': 'Site Architecture',
  'module_10': 'Branded vs Non-Branded',
  'module_11': 'Competitive Threat Radar',
  'module_12': 'Revenue Attribution'
};

const MODULE_ORDER = [
  'module_1', 'module_2', 'module_3', 'module_4', 'module_5', 'module_6',
  'module_7', 'module_8', 'module_9', 'module_10', 'module_11', 'module_12'
];

export default function ProgressPage() {
  const [report, setReport] = useState<Report | null>(null);
  const [steps, setSteps] = useState<ProgressStep[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedSteps, setExpandedSteps] = useState<Set<string>>(new Set());
  const [lastUpdated, setLastUpdated] = useState<Date>(new Date());

  useEffect(() => {
    const reportId = new URLSearchParams(window.location.search).get('id');
    if (!reportId) {
      setError('No report ID provided');
      setLoading(false);
      return;
    }

    fetchProgress(reportId);
    const interval = setInterval(() => fetchProgress(reportId), 3000);

    return () => clearInterval(interval);
  }, []);

  const fetchProgress = async (reportId: string) => {
    try {
      const response = await fetch(`/api/reports/${reportId}/progress`, {
        headers: {
          'Authorization': `Bearer ${localStorage.getItem('token')}`
        }
      });

      if (!response.ok) {
        throw new Error('Failed to fetch progress');
      }

      const data = await response.json();
      setReport(data.report);
      setSteps(buildSteps(data.report));
      setLastUpdated(new Date());
      setLoading(false);

      if (data.report.status === 'complete' || data.report.status === 'failed') {
        // Stop polling
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch progress');
      setLoading(false);
    }
  };

  const buildSteps = (report: Report): ProgressStep[] => {
    return MODULE_ORDER.map(moduleKey => ({
      module: MODULE_NAMES[moduleKey] || moduleKey,
      status: report.progress[moduleKey] || 'pending',
      startedAt: report.progress[`${moduleKey}_started`],
      completedAt: report.progress[`${moduleKey}_completed`],
      error: report.progress[`${moduleKey}_error`]
    }));
  };

  const toggleStep = (moduleKey: string) => {
    const newExpanded = new Set(expandedSteps);
    if (newExpanded.has(moduleKey)) {
      newExpanded.delete(moduleKey);
    } else {
      newExpanded.add(moduleKey);
    }
    setExpandedSteps(newExpanded);
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'complete':
        return <CheckCircle className="w-5 h-5 sm:w-6 sm:h-6 text-green-500 flex-shrink-0" />;
      case 'running':
        return <Loader2 className="w-5 h-5 sm:w-6 sm:h-6 text-blue-500 animate-spin flex-shrink-0" />;
      case 'failed':
        return <AlertCircle className="w-5 h-5 sm:w-6 sm:h-6 text-red-500 flex-shrink-0" />;
      default:
        return <Clock className="w-5 h-5 sm:w-6 sm:h-6 text-gray-400 flex-shrink-0" />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'complete':
        return 'bg-green-50 border-green-200';
      case 'running':
        return 'bg-blue-50 border-blue-200';
      case 'failed':
        return 'bg-red-50 border-red-200';
      default:
        return 'bg-gray-50 border-gray-200';
    }
  };

  const calculateProgress = () => {
    const total = steps.length;
    const completed = steps.filter(s => s.status === 'complete').length;
    return Math.round((completed / total) * 100);
  };

  const formatDuration = (ms: number) => {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    return minutes > 0 ? `${minutes}m ${remainingSeconds}s` : `${seconds}s`;
  };

  const formatTime = (timestamp?: string) => {
    if (!timestamp) return 'N/A';
    return new Date(timestamp).toLocaleTimeString();
  };

  if (loading && !report) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 flex items-center justify-center p-4">
        <div className="text-center">
          <Loader2 className="w-12 h-12 text-blue-600 animate-spin mx-auto mb-4" />
          <p className="text-gray-600">Loading report progress...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 flex items-center justify-center p-4">
        <div className="bg-white rounded-lg shadow-lg p-6 sm:p-8 max-w-md w-full">
          <AlertCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
          <h2 className="text-xl font-bold text-gray-900 mb-2 text-center">Error</h2>
          <p className="text-gray-600 text-center mb-4">{error}</p>
          <Link
            href="/dashboard"
            className="block w-full text-center bg-blue-600 text-white py-2 px-4 rounded-lg hover:bg-blue-700 transition-colors"
          >
            Back to Dashboard
          </Link>
        </div>
      </div>
    );
  }

  const progress = calculateProgress();
  const isComplete = report?.status === 'complete';
  const isFailed = report?.status === 'failed';

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100">
      <div className="max-w-4xl mx-auto px-4 py-6 sm:px-6 sm:py-8 lg:px-8">
        {/* Header */}
        <div className="bg-white rounded-lg shadow-lg p-4 sm:p-6 mb-4 sm:mb-6">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4 mb-4">
            <div className="min-w-0 flex-1">
              <h1 className="text-xl sm:text-2xl font-bold text-gray-900 mb-1 truncate">
                Report Generation Progress
              </h1>
              <p className="text-sm text-gray-600 truncate">{report?.gscProperty}</p>
            </div>
            {isComplete && (
              <Link
                href={`/report?id=${report?.id}`}
                className="flex items-center justify-center gap-2 bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 transition-colors whitespace-nowrap text-sm sm:text-base"
              >
                View Report <ExternalLink className="w-4 h-4" />
              </Link>
            )}
          </div>

          {/* Progress Bar */}
          <div className="mb-4">
            <div className="flex justify-between items-center mb-2">
              <span className="text-sm font-medium text-gray-700">Overall Progress</span>
              <span className="text-sm font-bold text-gray-900">{progress}%</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-3 sm:h-4 overflow-hidden">
              <div
                className={`h-full rounded-full transition-all duration-500 ${
                  isComplete ? 'bg-green-500' : isFailed ? 'bg-red-500' : 'bg-blue-500'
                }`}
                style={{ width: `${progress}%` }}
              />
            </div>
          </div>

          {/* Status Message */}
          <div className={`p-3 sm:p-4 rounded-lg ${
            isComplete ? 'bg-green-50 border border-green-200' :
            isFailed ? 'bg-red-50 border border-red-200' :
            'bg-blue-50 border border-blue-200'
          }`}>
            <p className="text-sm sm:text-base font-medium text-gray-900">
              {isComplete && '✓ Report generation complete!'}
              {isFailed && '✕ Report generation failed'}
              {!isComplete && !isFailed && 'Generating your Search Intelligence Report...'}
            </p>
            {!isComplete && !isFailed && (
              <p className="text-xs sm:text-sm text-gray-600 mt-1">
                This typically takes 2-5 minutes. You can leave this page and come back later.
              </p>
            )}
          </div>

          {/* Last Updated */}
          <div className="mt-3 text-xs text-gray-500 text-right">
            Last updated: {lastUpdated.toLocaleTimeString()}
          </div>
        </div>

        {/* Module Progress Steps */}
        <div className="space-y-2 sm:space-y-3">
          {steps.map((step, index) => {
            const moduleKey = MODULE_ORDER[index];
            const isExpanded = expandedSteps.has(moduleKey);
            const hasDetails = step.error || step.startedAt || step.completedAt;

            return (
              <div
                key={moduleKey}
                className={`bg-white rounded-lg shadow border-2 transition-all ${getStatusColor(step.status)}`}
              >
                <button
                  onClick={() => hasDetails && toggleStep(moduleKey)}
                  className={`w-full p-3 sm:p-4 flex items-start gap-3 sm:gap-4 text-left ${
                    hasDetails ? 'cursor-pointer hover:bg-opacity-50' : 'cursor-default'
                  }`}
                  disabled={!hasDetails}
                >
                  {getStatusIcon(step.status)}
                  <div className="flex-1 min-w-0">
                    <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-1 sm:gap-2">
                      <h3 className="text-sm sm:text-base font-semibold text-gray-900 pr-2">
                        {step.module}
                      </h3>
                      {step.status === 'running' && (
                        <span className="text-xs text-blue-600 font-medium whitespace-nowrap">
                          In progress...
                        </span>
                      )}
                    </div>
                  </div>
                  {hasDetails && (
                    <div className="flex-shrink-0">
                      {isExpanded ? (
                        <ChevronUp className="w-5 h-5 text-gray-400" />
                      ) : (
                        <ChevronDown className="w-5 h-5 text-gray-400" />
                      )}
                    </div>
                  )}
                </button>

                {isExpanded && hasDetails && (
                  <div className="px-3 sm:px-4 pb-3 sm:pb-4 border-t border-gray-200 mt-2 pt-3">
                    <div className="space-y-2 text-xs sm:text-sm">
                      {step.startedAt && (
                        <div className="flex flex-col sm:flex-row sm:justify-between gap-1">
                          <span className="text-gray-600">Started:</span>
                          <span className="text-gray-900 font-medium">{formatTime(step.startedAt)}</span>
                        </div>
                      )}
                      {step.completedAt && (
                        <div className="flex flex-col sm:flex-row sm:justify-between gap-1">
                          <span className="text-gray-600">Completed:</span>
                          <span className="text-gray-900 font-medium">{formatTime(step.completedAt)}</span>
                        </div>
                      )}
                      {step.duration && (
                        <div className="flex flex-col sm:flex-row sm:justify-between gap-1">
                          <span className="text-gray-600">Duration:</span>
                          <span className="text-gray-900 font-medium">{formatDuration(step.duration)}</span>
                        </div>
                      )}
                      {step.error && (
                        <div className="mt-2 p-2 bg-red-100 border border-red-300 rounded text-red-800">
                          <strong>Error:</strong> {step.error}
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>

        {/* Summary Stats - Mobile Optimized Cards */}
        <div className="mt-6 grid grid-cols-2 sm:grid-cols-3 gap-3 sm:gap-4">
          <div className="bg-white rounded-lg shadow p-3 sm:p-4">
            <div className="text-xs sm:text-sm text-gray-600 mb-1">Completed</div>
            <div className="text-xl sm:text-2xl font-bold text-green-600">
              {steps.filter(s => s.status === 'complete').length}
            </div>
          </div>
          <div className="bg-white rounded-lg shadow p-3 sm:p-4">
            <div className="text-xs sm:text-sm text-gray-600 mb-1">In Progress</div>
            <div className="text-xl sm:text-2xl font-bold text-blue-600">
              {steps.filter(s => s.status === 'running').length}
            </div>
          </div>
          <div className="bg-white rounded-lg shadow p-3 sm:p-4 col-span-2 sm:col-span-1">
            <div className="text-xs sm:text-sm text-gray-600 mb-1">Remaining</div>
            <div className="text-xl sm:text-2xl font-bold text-gray-600">
              {steps.filter(s => s.status === 'pending').length}
            </div>
          </div>
        </div>

        {/* Back to Dashboard Link */}
        <div className="mt-6 text-center">
          <Link
            href="/dashboard"
            className="text-sm sm:text-base text-blue-600 hover:text-blue-700 font-medium"
          >
            ← Back to Dashboard
          </Link>
        </div>
      </div>
    </div>
  );
}
