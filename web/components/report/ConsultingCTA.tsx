import React from 'react';
import { Calendar, TrendingUp, Target } from 'lucide-react';

interface ConsultingCTAProps {
  estimatedMonthlyValue?: number;
  actionCount?: number;
}

export default function ConsultingCTA({ 
  estimatedMonthlyValue,
  actionCount 
}: ConsultingCTAProps) {
  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  };

  const handleBookCall = () => {
    // TODO: Integrate with Calendly or booking system
    window.open('https://calendly.com/your-booking-link', '_blank');
  };

  return (
    <div className="my-12 rounded-xl bg-gradient-to-br from-blue-50 to-indigo-50 dark:from-blue-950/30 dark:to-indigo-950/30 border border-blue-200 dark:border-blue-800 overflow-hidden">
      <div className="p-8 md:p-10">
        <div className="flex items-start gap-4 mb-6">
          <div className="flex-shrink-0 w-12 h-12 rounded-full bg-blue-600 dark:bg-blue-500 flex items-center justify-center">
            <Target className="w-6 h-6 text-white" />
          </div>
          <div className="flex-1">
            <h3 className="text-2xl font-bold text-gray-900 dark:text-gray-100 mb-2">
              Want Help Executing This Plan?
            </h3>
            <p className="text-gray-700 dark:text-gray-300 text-lg">
              You now have a comprehensive roadmap. Let's work together to implement it and capture these opportunities.
            </p>
          </div>
        </div>

        {(estimatedMonthlyValue || actionCount) && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
            {estimatedMonthlyValue && (
              <div className="bg-white dark:bg-gray-900 rounded-lg p-6 border border-blue-100 dark:border-blue-900">
                <div className="flex items-center gap-3 mb-2">
                  <TrendingUp className="w-5 h-5 text-green-600 dark:text-green-500" />
                  <span className="text-sm font-medium text-gray-600 dark:text-gray-400 uppercase tracking-wide">
                    Monthly Opportunity
                  </span>
                </div>
                <div className="text-3xl font-bold text-gray-900 dark:text-gray-100">
                  {formatCurrency(estimatedMonthlyValue)}
                </div>
                <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                  in recoverable and growth traffic value
                </p>
              </div>
            )}

            {actionCount && (
              <div className="bg-white dark:bg-gray-900 rounded-lg p-6 border border-blue-100 dark:border-blue-900">
                <div className="flex items-center gap-3 mb-2">
                  <Calendar className="w-5 h-5 text-blue-600 dark:text-blue-500" />
                  <span className="text-sm font-medium text-gray-600 dark:text-gray-400 uppercase tracking-wide">
                    Action Items
                  </span>
                </div>
                <div className="text-3xl font-bold text-gray-900 dark:text-gray-100">
                  {actionCount}
                </div>
                <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                  prioritized recommendations ready to implement
                </p>
              </div>
            )}
          </div>
        )}

        <div className="space-y-4">
          <div className="bg-white dark:bg-gray-900 rounded-lg p-6 border border-blue-100 dark:border-blue-900">
            <h4 className="font-semibold text-gray-900 dark:text-gray-100 mb-3">
              What You Get in a Consulting Engagement:
            </h4>
            <ul className="space-y-2 text-gray-700 dark:text-gray-300">
              <li className="flex items-start gap-2">
                <span className="text-blue-600 dark:text-blue-400 mt-1">✓</span>
                <span>Direct implementation support for critical fixes and quick wins</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-blue-600 dark:text-blue-400 mt-1">✓</span>
                <span>Monthly progress tracking with updated reports and adjusted strategy</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-blue-600 dark:text-blue-400 mt-1">✓</span>
                <span>Content strategy and optimization guidance for striking distance keywords</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-blue-600 dark:text-blue-400 mt-1">✓</span>
                <span>Technical SEO architecture improvements and internal linking strategy</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-blue-600 dark:text-blue-400 mt-1">✓</span>
                <span>Competitive monitoring and algorithm update response planning</span>
              </li>
            </ul>
          </div>

          <div className="flex flex-col sm:flex-row gap-4">
            <button
              onClick={handleBookCall}
              className="flex-1 bg-blue-600 hover:bg-blue-700 dark:bg-blue-500 dark:hover:bg-blue-600 text-white font-semibold py-4 px-6 rounded-lg transition-colors duration-200 shadow-lg shadow-blue-600/25 dark:shadow-blue-500/25"
            >
              Book a Strategy Call
            </button>
            <a
              href="mailto:your@email.com"
              className="flex-1 bg-white dark:bg-gray-900 hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-900 dark:text-gray-100 font-semibold py-4 px-6 rounded-lg transition-colors duration-200 border-2 border-gray-300 dark:border-gray-700 text-center"
            >
              Email Instead
            </a>
          </div>

          <p className="text-sm text-gray-600 dark:text-gray-400 text-center">
            30-minute consultation to review your report and discuss engagement options — no obligation
          </p>
        </div>
      </div>

      <div className="bg-blue-100 dark:bg-blue-900/30 px-8 py-4 border-t border-blue-200 dark:border-blue-800">
        <p className="text-sm text-gray-700 dark:text-gray-300 text-center">
          <strong>Typical ROI:</strong> Clients see 3-5x return on consulting investment within the first 90 days through implementation of critical fixes and quick wins identified in reports like this.
        </p>
      </div>
    </div>
  );
}
