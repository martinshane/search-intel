import React from 'react';
import Head from 'next/head';
import Link from 'next/link';

/**
 * Custom 404 page matching the app\'s dark theme.
 *
 * Without this file, Next.js renders its default white 404 page
 * which clashes with the dark theme used across all other pages.
 * This page provides:
 * - Consistent dark theme
 * - Clear "not found" messaging
 * - Navigation back to home and reports
 * - Mobile responsive layout
 */
export default function Custom404() {
  return (
    <>
      <Head>
        <title>Page Not Found — Search Intelligence Report</title>
        <meta name="robots" content="noindex" />
      </Head>

      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center px-4">
        <div className="text-center max-w-md">
          {/* Large 404 indicator */}
          <div className="text-8xl font-bold text-slate-700 mb-4 select-none">
            404
          </div>

          <h1 className="text-2xl sm:text-3xl font-bold text-white mb-3">
            Page not found
          </h1>

          <p className="text-slate-400 mb-8 leading-relaxed">
            The page you\u2019re looking for doesn\u2019t exist or has been moved.
            If you followed a link here, it may be outdated.
          </p>

          {/* Navigation options */}
          <div className="flex flex-col sm:flex-row gap-3 justify-center">
            <Link
              href="/"
              className="inline-flex items-center justify-center px-6 py-3 bg-indigo-600 hover:bg-indigo-500 text-white font-medium rounded-lg transition-colors"
            >
              Go to Home
            </Link>

            <Link
              href="/reports"
              className="inline-flex items-center justify-center px-6 py-3 bg-slate-700 hover:bg-slate-600 text-slate-200 font-medium rounded-lg transition-colors"
            >
              My Reports
            </Link>
          </div>

          {/* Subtle branding */}
          <p className="mt-12 text-sm text-slate-600">
            Search Intelligence Report by{\u0027 \u0027'}
            <a
              href="https://clankermarketing.com"
              className="text-slate-500 hover:text-slate-400 transition-colors"
              target="_blank"
              rel="noopener noreferrer"
            >
              Clanker Marketing
            </a>
          </p>
        </div>
      </div>
    </>
  );
}