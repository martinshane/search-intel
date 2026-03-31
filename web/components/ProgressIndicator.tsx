import React from 'react';

interface ProgressStep {
  id: string;
  label: string;
  status: 'pending' | 'running' | 'complete' | 'failed';
  startedAt?: string;
  completedAt?: string;
}

interface ProgressIndicatorProps {
  steps: ProgressStep[];
  currentStep?: string;
  className?: string;
}

const ProgressIndicator: React.FC<ProgressIndicatorProps> = ({
  steps,
  currentStep,
  className = '',
}) => {
  const getStepIcon = (status: ProgressStep['status']) => {
    switch (status) {
      case 'complete':
        return (
          <svg
            className="w-5 h-5 text-green-500"
            fill="currentColor"
            viewBox="0 0 20 20"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              fillRule="evenodd"
              d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
              clipRule="evenodd"
            />
          </svg>
        );
      case 'running':
        return (
          <svg
            className="w-5 h-5 text-blue-500 animate-spin"
            fill="none"
            viewBox="0 0 24 24"
            xmlns="http://www.w3.org/2000/svg"
          >
            <circle
              className="opacity-25"
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
            />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
            />
          </svg>
        );
      case 'failed':
        return (
          <svg
            className="w-5 h-5 text-red-500"
            fill="currentColor"
            viewBox="0 0 20 20"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              fillRule="evenodd"
              d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
              clipRule="evenodd"
            />
          </svg>
        );
      default:
        return (
          <div className="w-5 h-5 rounded-full border-2 border-gray-300 bg-white" />
        );
    }
  };

  const getStepClasses = (status: ProgressStep['status'], isActive: boolean) => {
    const baseClasses = 'flex items-center space-x-3 px-4 py-3 rounded-lg transition-all duration-200';
    
    if (status === 'failed') {
      return `${baseClasses} bg-red-50 border border-red-200`;
    }
    
    if (status === 'running' || isActive) {
      return `${baseClasses} bg-blue-50 border border-blue-200`;
    }
    
    if (status === 'complete') {
      return `${baseClasses} bg-green-50 border border-green-200`;
    }
    
    return `${baseClasses} bg-gray-50 border border-gray-200`;
  };

  const getStepTextClasses = (status: ProgressStep['status']) => {
    switch (status) {
      case 'complete':
        return 'text-green-900 font-medium';
      case 'running':
        return 'text-blue-900 font-semibold';
      case 'failed':
        return 'text-red-900 font-medium';
      default:
        return 'text-gray-600';
    }
  };

  const formatDuration = (startedAt?: string, completedAt?: string) => {
    if (!startedAt) return null;
    
    const start = new Date(startedAt).getTime();
    const end = completedAt ? new Date(completedAt).getTime() : Date.now();
    const durationMs = end - start;
    
    if (durationMs < 1000) {
      return '<1s';
    }
    
    const seconds = Math.floor(durationMs / 1000);
    
    if (seconds < 60) {
      return `${seconds}s`;
    }
    
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    
    return `${minutes}m ${remainingSeconds}s`;
  };

  const completedSteps = steps.filter(s => s.status === 'complete').length;
  const totalSteps = steps.length;
  const progressPercentage = (completedSteps / totalSteps) * 100;

  return (
    <div className={`w-full ${className}`}>
      {/* Progress bar */}
      <div className="mb-6">
        <div className="flex items-center justify-between mb-2">
          <span className="text-sm font-medium text-gray-700">
            Generating Report
          </span>
          <span className="text-sm text-gray-600">
            {completedSteps} of {totalSteps} modules complete
          </span>
        </div>
        <div className="w-full bg-gray-200 rounded-full h-2.5 overflow-hidden">
          <div
            className="bg-blue-600 h-2.5 rounded-full transition-all duration-500 ease-out"
            style={{ width: `${progressPercentage}%` }}
          />
        </div>
      </div>

      {/* Step list */}
      <div className="space-y-2">
        {steps.map((step) => {
          const isActive = currentStep === step.id;
          const duration = formatDuration(step.startedAt, step.completedAt);

          return (
            <div
              key={step.id}
              className={getStepClasses(step.status, isActive)}
            >
              <div className="flex-shrink-0">
                {getStepIcon(step.status)}
              </div>
              
              <div className="flex-grow min-w-0">
                <p className={`text-sm truncate ${getStepTextClasses(step.status)}`}>
                  {step.label}
                </p>
              </div>
              
              {duration && (
                <div className="flex-shrink-0">
                  <span className="text-xs text-gray-500">
                    {duration}
                  </span>
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Status message */}
      {steps.some(s => s.status === 'running') && (
        <div className="mt-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
          <div className="flex items-start space-x-3">
            <svg
              className="w-5 h-5 text-blue-500 mt-0.5 flex-shrink-0"
              fill="currentColor"
              viewBox="0 0 20 20"
              xmlns="http://www.w3.org/2000/svg"
            >
              <path
                fillRule="evenodd"
                d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z"
                clipRule="evenodd"
              />
            </svg>
            <div className="flex-grow">
              <p className="text-sm font-medium text-blue-900">
                Analysis in progress
              </p>
              <p className="text-sm text-blue-700 mt-1">
                This typically takes 2-5 minutes. You can leave this page and we'll email you when it's ready.
              </p>
            </div>
          </div>
        </div>
      )}

      {steps.some(s => s.status === 'failed') && (
        <div className="mt-6 p-4 bg-red-50 border border-red-200 rounded-lg">
          <div className="flex items-start space-x-3">
            <svg
              className="w-5 h-5 text-red-500 mt-0.5 flex-shrink-0"
              fill="currentColor"
              viewBox="0 0 20 20"
              xmlns="http://www.w3.org/2000/svg"
            >
              <path
                fillRule="evenodd"
                d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
                clipRule="evenodd"
              />
            </svg>
            <div className="flex-grow">
              <p className="text-sm font-medium text-red-900">
                Analysis failed
              </p>
              <p className="text-sm text-red-700 mt-1">
                One or more modules encountered an error. Please try again or contact support if the issue persists.
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ProgressIndicator;
