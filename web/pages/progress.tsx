import React, { useEffect, useState } from 'react';
import { useRouter } from 'next/router';
import Head from 'next/head';

interface ProgressState {
  status: 'pending' | 'ingesting' | 'analyzing' | 'generating' | 'complete' | 'failed';
  progress: {
    module_1?: 'pending' | 'running' | 'complete' | 'failed';
    module_2?: 'pending' | 'running' | 'complete' | 'failed';
    module_3?: 'pending' | 'running' | 'complete' | 'failed';
    module_4?: 'pending' | 'running' | 'complete' | 'failed';
    module_5?: 'pending' | 'running' | 'complete' | 'failed';
    module_6?: 'pending' | 'running' | 'complete' | 'failed';
    module_7?: 'pending' | 'running' | 'complete' | 'failed';
    module_8?: 'pending' | 'running' | 'complete' | 'failed';
    module_9?: 'pending' | 'running' | 'complete' | 'failed';
    module_10?: 'pending' | 'running' | 'complete' | 'failed';
    module_11?: 'pending' | 'running' | 'complete' | 'failed';
    module_12?: 'pending' | 'running' | 'complete' | 'failed';
  };
  error?: string;
}

const MODULE_NAMES: Record<string, string> = {
  module_1: 'Health & Trajectory Analysis',
  module_2: 'Page-Level Triage',
  module_3: 'SERP Landscape Analysis',
  module_4: 'Content Intelligence',
  module_5: 'Strategic Gameplan',
  module_6: 'Algorithm Update Impact',
  module_7: 'Query Intent Migration',
  module_8: 'CTR Modeling by SERP Context',
  module_9: 'Site Architecture & Authority Flow',
  module_10: 'Branded vs Non-Branded Health',
  module_11: 'Competitive Threat Radar',
  module_12: 'Revenue Attribution',
};

const PHASE_DESCRIPTIONS: Record<string, string> = {
  pending: 'Initializing report generation...',
  ingesting: 'Fetching data from Google Search Console, GA4, and DataForSEO...',
  analyzing: 'Running analysis modules...',
  generating: 'Synthesizing insights and generating report...',
  complete: 'Report ready!',
  failed: 'Report generation failed',
};

export default function ProgressPage() {
  const router = useRouter();
  const { jobId } = router.query;
  const [progressState, setProgressState] = useState<ProgressState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [elapsedTime, setElapsedTime] = useState(0);

  useEffect(() => {
    if (!jobId) return;

    let pollInterval: NodeJS.Timeout;
    let timeInterval: NodeJS.Timeout;

    const pollJobStatus = async () => {
      try {
        const response = await fetch(`/api/jobs/${jobId}/status`, {
          credentials: 'include',
        });

        if (!response.ok) {
          if (response.status === 404) {
            setError('Job not found. It may have expired or been deleted.');
            return;
          }
          throw new Error(`Failed to fetch job status: ${response.statusText}`);
        }

        const data: ProgressState = await response.json();
        setProgressState(data);

        if (data.status === 'complete') {
          clearInterval(pollInterval);
          clearInterval(timeInterval);
          // Redirect to report page
          router.push(`/report/${jobId}`);
        } else if (data.status === 'failed') {
          clearInterval(pollInterval);
          clearInterval(timeInterval);
          setError(data.error || 'Report generation failed. Please try again.');
        }
      } catch (err) {
        console.error('Error polling job status:', err);
        setError(err instanceof Error ? err.message : 'Failed to check job status');
        clearInterval(pollInterval);
        clearInterval(timeInterval);
      }
    };

    // Initial poll
    pollJobStatus();

    // Poll every 5 seconds
    pollInterval = setInterval(pollJobStatus, 5000);

    // Update elapsed time every second
    timeInterval = setInterval(() => {
      setElapsedTime((prev) => prev + 1);
    }, 1000);

    return () => {
      clearInterval(pollInterval);
      clearInterval(timeInterval);
    };
  }, [jobId, router]);

  const formatTime = (seconds: number): string => {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins}:${secs.toString().padStart(2, '0')}`;
  };

  const getModuleStatus = (moduleKey: string): 'pending' | 'running' | 'complete' | 'failed' => {
    if (!progressState?.progress) return 'pending';
    return progressState.progress[moduleKey as keyof typeof progressState.progress] || 'pending';
  };

  const getModuleIcon = (status: string): string => {
    switch (status) {
      case 'complete':
        return '✓';
      case 'running':
        return '⟳';
      case 'failed':
        return '✗';
      default:
        return '○';
    }
  };

  const getModuleColor = (status: string): string => {
    switch (status) {
      case 'complete':
        return 'text-green-600';
      case 'running':
        return 'text-blue-600 animate-spin';
      case 'failed':
        return 'text-red-600';
      default:
        return 'text-gray-400';
    }
  };

  const calculateOverallProgress = (): number => {
    if (!progressState?.progress) return 0;
    
    const modules = Object.keys(MODULE_NAMES);
    const completedCount = modules.filter(
      (key) => getModuleStatus(key) === 'complete'
    ).length;
    
    return Math.round((completedCount / modules.length) * 100);
  };

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center px-4">
        <Head>
          <title>Error - Search Intelligence Report</title>
        </Head>
        <div className="max-w-md w-full bg-white rounded-lg shadow-lg p-8">
          <div className="text-center">
            <div className="text-red-600 text-5xl mb-4">✗</div>
            <h1 className="text-2xl font-bold text-gray-900 mb-4">
              Generation Failed
            </h1>
            <p className="text-gray-600 mb-6">{error}</p>
            <button
              onClick={() => router.push('/dashboard')}
              className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors"
            >
              Return to Dashboard
            </button>
          </div>
        </div>
      </div>
    );
  }

  if (!progressState) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <Head>
          <title>Loading - Search Intelligence Report</title>
        </Head>
        <div className="text-center">
          <div className="animate-spin text-6xl text-blue-600 mb-4">⟳</div>
          <p className="text-gray-600">Initializing...</p>
        </div>
      </div>
    );
  }

  const overallProgress = calculateOverallProgress();

  return (
    <div className="min-h-screen bg-gray-50 py-12 px-4">
      <Head>
        <title>Generating Report - Search Intelligence Report</title>
      </Head>
      
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
          <div className="text-center mb-6">
            <h1 className="text-3xl font-bold text-gray-900 mb-2">
              Generating Your Search Intelligence Report
            </h1>
            <p className="text-gray-600">
              {PHASE_DESCRIPTIONS[progressState.status]}
            </p>
          </div>

          {/* Overall Progress Bar */}
          <div className="mb-4">
            <div className="flex justify-between items-center mb-2">
              <span className="text-sm font-medium text-gray-700">
                Overall Progress
              </span>
              <span className="text-sm font-medium text-gray-700">
                {overallProgress}%
              </span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-3 overflow-hidden">
              <div
                className="bg-blue-600 h-full transition-all duration-500 ease-out"
                style={{ width: `${overallProgress}%` }}
              />
            </div>
          </div>

          {/* Elapsed Time */}
          <div className="text-center text-sm text-gray-500 mt-4">
            Elapsed time: {formatTime(elapsedTime)}
            <span className="ml-4 text-gray-400">
              (Estimated: 2-5 minutes)
            </span>
          </div>
        </div>

        {/* Module Progress */}
        <div className="bg-white rounded-lg shadow-lg p-8">
          <h2 className="text-xl font-bold text-gray-900 mb-6">
            Analysis Modules
          </h2>
          
          <div className="space-y-4">
            {Object.entries(MODULE_NAMES).map(([moduleKey, moduleName]) => {
              const status = getModuleStatus(moduleKey);
              const icon = getModuleIcon(status);
              const colorClass = getModuleColor(status);
              
              return (
                <div
                  key={moduleKey}
                  className={`flex items-center justify-between p-4 rounded-lg border-2 transition-all ${
                    status === 'running'
                      ? 'border-blue-300 bg-blue-50'
                      : status === 'complete'
                      ? 'border-green-300 bg-green-50'
                      : status === 'failed'
                      ? 'border-red-300 bg-red-50'
                      : 'border-gray-200 bg-gray-50'
                  }`}
                >
                  <div className="flex items-center gap-4">
                    <span className={`text-2xl ${colorClass}`}>
                      {icon}
                    </span>
                    <div>
                      <h3 className="font-semibold text-gray-900">
                        {moduleName}
                      </h3>
                      <p className="text-sm text-gray-500 capitalize">
                        {status}
                      </p>
                    </div>
                  </div>
                  
                  {status === 'running' && (
                    <div className="flex items-center gap-2 text-blue-600">
                      <div className="animate-pulse">●</div>
                      <span className="text-sm font-medium">
                        Processing...
                      </span>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>

        {/* Info Box */}
        <div className="mt-8 bg-blue-50 border-2 border-blue-200 rounded-lg p-6">
          <div className="flex items-start gap-4">
            <div className="text-blue-600 text-2xl">ℹ</div>
            <div>
              <h3 className="font-semibold text-blue-900 mb-2">
                What's happening behind the scenes?
              </h3>
              <ul className="text-sm text-blue-800 space-y-1">
                <li>• Fetching 16 months of data from Google Search Console & GA4</li>
                <li>• Pulling live SERP data for your top keywords</li>
                <li>• Running statistical analysis and machine learning models</li>
                <li>• Analyzing site architecture and internal link graph</li>
                <li>• Synthesizing insights across 12 analysis dimensions</li>
              </ul>
            </div>
          </div>
        </div>

        {/* Safe to close notice */}
        <div className="mt-6 text-center text-sm text-gray-500">
          You can safely close this page. We'll email you when the report is ready.
        </div>
      </div>
    </div>
  );
}
