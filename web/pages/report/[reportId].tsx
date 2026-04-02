/**
 * TOMBSTONE — DO NOT USE
 *
 * This file was the original report viewer using [reportId] as the dynamic
 * route parameter.  It has been superseded by [id].tsx which contains the
 * canonical, fully-featured report viewer with all 12 module visualizations.
 *
 * Having both [id].tsx and [reportId].tsx in the same Next.js directory
 * causes a build error:
 *   "You cannot use different slug names for the same dynamic path"
 *
 * This file is kept as a tombstone redirect to prevent accidental recreation.
 * It simply redirects any /report/[reportId] traffic to the canonical viewer.
 *
 * Replaced by: web/pages/report/[id].tsx (196K chars, 12 module charts)
 * Tombstoned:  2026-04-02
 */
import { useRouter } from 'next/router';
import { useEffect } from 'react';

export default function ReportIdRedirect() {
  const router = useRouter();
  const { reportId } = router.query;

  useEffect(() => {
    // This page should never be reached because [id].tsx handles all
    // dynamic /report/* routes.  If it IS reached somehow, redirect
    // to the reports list.
    if (router.isReady) {
      router.replace('/reports');
    }
  }, [router, reportId]);

  return null;
}
