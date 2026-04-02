/** @type {import('next').NextConfig} */

// -----------------------------------------------------------------------
// Backend URL resolution (used by the server-side rewrite proxy).
//
// BACKEND_URL is a server-only env var — it is NOT exposed to the browser.
// NEXT_PUBLIC_API_URL is the public env var that pages read at build time.
//
// Priority for the proxy destination:
//   1. BACKEND_URL          (server-side only, preferred for Railway)
//   2. NEXT_PUBLIC_API_URL  (build-time, also used by frontend pages)
//   3. http://localhost:8000 (local dev fallback)
// -----------------------------------------------------------------------
const backendUrl =
  process.env.BACKEND_URL ||
  process.env.NEXT_PUBLIC_API_URL ||
  'http://localhost:8000';

const nextConfig = {
  reactStrictMode: true,
  env: {
    // Expose the API URL to frontend pages.
    // CRITICAL: The fallback is '' (empty string), NOT 'http://localhost:8000'.
    //
    // When NEXT_PUBLIC_API_URL is set (e.g. to the Railway API service URL),
    // frontend pages make direct cross-origin calls to the backend.
    //
    // When NEXT_PUBLIC_API_URL is NOT set, the empty fallback means pages
    // construct relative URLs like '/api/auth/status'.  These hit the
    // Next.js server, which proxies them to the backend via the rewrite
    // rule below.
    //
    // The old fallback was 'http://localhost:8000' which got baked into
    // the production bundle — making EVERY API call fail from the user's
    // browser (localhost:8000 doesn't exist on their machine).
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || '',
  },
  async rewrites() {
    const rewrites = [
      // ---------------------------------------------------------------
      // Frontend route aliases
      // ---------------------------------------------------------------
      // All frontend links use /reports/:id (plural) but the page file
      // lives at pages/report/[id].tsx (/report/:id singular).
      // This rewrite bridges the gap so both paths resolve correctly.
      {
        source: '/reports/:path*',
        destination: '/report/:path*',
      },
      // index.tsx links to /dashboard but the page is /progress
      {
        source: '/dashboard',
        destination: '/progress',
      },
    ];

    // ---------------------------------------------------------------
    // API proxy rewrite — active in ALL environments.
    //
    // In development: proxies /api/* to localhost:8000/api/*
    // In production:  proxies /api/* to BACKEND_URL/api/*
    //
    // This makes the frontend work without NEXT_PUBLIC_API_URL set
    // (relative /api/* requests are proxied server-side).  When
    // NEXT_PUBLIC_API_URL IS set, the frontend calls the API directly
    // and this rewrite is never hit (pages use absolute URLs).
    // ---------------------------------------------------------------
    rewrites.push({
      source: '/api/:path*',
      destination: `${backendUrl}/api/:path*`,
    });

    // Also proxy /health so the frontend can healthcheck the backend
    rewrites.push({
      source: '/health',
      destination: `${backendUrl}/health`,
    });

    return rewrites;
  },
  // Enable SWC minification for better performance
  swcMinify: true,
  // Configure webpack for better build optimization
  webpack: (config, { isServer }) => {
    if (!isServer) {
      // Don't bundle server-only modules on client
      config.resolve.fallback = {
        ...config.resolve.fallback,
        fs: false,
        net: false,
        tls: false,
      };
    }
    return config;
  },
  // Image optimization config (if needed later for reports)
  images: {
    domains: ['lh3.googleusercontent.com'], // Google profile images from OAuth
    formats: ['image/avif', 'image/webp'],
  },
  // Security headers
  async headers() {
    return [
      {
        source: '/(.*)',
        headers: [
          {
            key: 'X-Frame-Options',
            value: 'DENY',
          },
          {
            key: 'X-Content-Type-Options',
            value: 'nosniff',
          },
          {
            key: 'Referrer-Policy',
            value: 'strict-origin-when-cross-origin',
          },
        ],
      },
    ];
  },
  // Production-only optimizations
  ...(process.env.NODE_ENV === 'production' && {
    compiler: {
      removeConsole: {
        exclude: ['error', 'warn'],
      },
    },
  }),
};

module.exports = nextConfig;
