/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  env: {
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000',
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

    // In development, also proxy API requests to the backend
    if (process.env.NODE_ENV === 'development') {
      rewrites.push({
        source: '/api/:path*',
        destination: `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/api/:path*`,
      });
    }

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
