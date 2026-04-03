import React, { Component, ErrorInfo } from 'react';
import { AlertTriangle, RefreshCw } from 'lucide-react';

interface ErrorBoundaryProps {
  children: React.ReactNode;
  /** Human-readable label shown when an error occurs (e.g. "Module 3: SERP Landscape") */
  label?: string;
  /** Optional CSS class on the fallback container */
  className?: string;
}

interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

/**
 * React Error Boundary that catches rendering errors in its children and
 * displays a graceful fallback UI instead of crashing the entire page.
 *
 * Usage:
 *   <ErrorBoundary label="Health & Trajectory">
 *     <HealthTrajectoryContent data={data} />
 *   </ErrorBoundary>
 *
 * If HealthTrajectoryContent throws during render, the boundary catches
 * the error and shows a styled fallback card — the rest of the report
 * continues to render normally.
 */
export default class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // Log for debugging — in production this could be sent to an error service
    console.error(
      `[ErrorBoundary] ${this.props.label || 'Unknown'} crashed:`,
      error,
      info.componentStack,
    );
  }

  handleRetry = () => {
    this.setState({ hasError: false, error: null });
  };

  render() {
    if (this.state.hasError) {
      return (
        <div className={`rounded-lg border border-amber-500/30 bg-amber-950/20 p-6 ${this.props.className || ''}`}>
          <div className="flex items-start space-x-3">
            <AlertTriangle className="w-5 h-5 text-amber-400 flex-shrink-0 mt-0.5" />
            <div className="flex-1 min-w-0">
              <h3 className="text-sm font-semibold text-amber-300">
                {this.props.label
                  ? `${this.props.label} — Rendering Error`
                  : 'Rendering Error'}
              </h3>
              <p className="mt-1 text-sm text-amber-200/70">
                This section encountered an error while rendering. The rest of the
                report is unaffected.
              </p>
              {this.state.error && (
                <pre className="mt-2 text-xs text-amber-200/50 font-mono whitespace-pre-wrap break-words max-h-24 overflow-y-auto">
                  {this.state.error.message}
                </pre>
              )}
              <button
                onClick={this.handleRetry}
                className="mt-3 inline-flex items-center space-x-1.5 text-xs font-medium text-amber-300 hover:text-amber-200 transition"
              >
                <RefreshCw className="w-3.5 h-3.5" />
                <span>Retry</span>
              </button>
            </div>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
