/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  env: {
    NEXT_PUBLIC_SUPABASE_URL: process.env.NEXT_PUBLIC_SUPABASE_URL,
    NEXT_PUBLIC_SUPABASE_ANON_KEY: process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY,
  },
  // Ensure environment variables are validated at build time
  webpack: (config, { isServer }) => {
    if (isServer) {
      // Validate required environment variables on server-side builds
      const required = [
        'NEXT_PUBLIC_SUPABASE_URL',
        'NEXT_PUBLIC_SUPABASE_ANON_KEY',
      ];
      
      const missing = required.filter(key => !process.env[key]);
      
      if (missing.length > 0) {
        throw new Error(
          `Missing required environment variables: ${missing.join(', ')}\n` +
          'Please check your .env.local file or deployment environment settings.'
        );
      }
    }
    
    return config;
  },
};

module.exports = nextConfig;