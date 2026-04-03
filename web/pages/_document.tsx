import { Html, Head, Main, NextScript } from 'next/document';

/**
 * Custom Document — sets dark background at the HTML/body level so
 * the browser paints a dark background BEFORE React hydrates.
 *
 * Without this, users see a white flash on every page load because
 * globals.css body background renders first, then Tailwind's dark
 * classes take effect after React hydration.
 *
 * Also sets:
 * - lang="en" for accessibility / SEO
 * - Proper meta tags for mobile and PWA
 * - Open Graph / Twitter Card defaults
 * - Favicon link
 */

const SITE_NAME = 'Search Intelligence Report';
const SITE_URL = 'https://clankermarketing.com';
const DEFAULT_DESCRIPTION =
  'Free 12-module SEO analysis tool. Connect Google Search Console and GA4 for health trajectory, page triage, SERP intelligence, competitive radar, revenue attribution, and more.';

export default function Document() {
  return (
    <Html lang="en" className="bg-slate-900">
      <Head>
        {/* Preconnect to external origins used by the app */}
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />

        {/* PWA / mobile meta */}
        <meta name="theme-color" content="#0f172a" />
        <meta name="apple-mobile-web-app-capable" content="yes" />
        <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent" />

        {/* Default Open Graph tags — pages override with their own <Head> */}
        <meta property="og:site_name" content={SITE_NAME} />
        <meta property="og:type" content="website" />
        <meta property="og:locale" content="en_US" />

        {/* Default Twitter Card tags */}
        <meta name="twitter:card" content="summary_large_image" />

        {/* Favicon — uses emoji shortcut until a real favicon is designed */}
        <link
          rel="icon"
          href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>🔍</text></svg>"
        />

        {/* Robots — allow full indexing */}
        <meta name="robots" content="index, follow, max-image-preview:large, max-snippet:-1, max-video-preview:-1" />
                <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
        </Head>
      <body className="bg-slate-900 text-white antialiased">
        <Main />
        <NextScript />
      </body>
    </Html>
  );
}
