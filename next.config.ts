/** @type {import('next').NextConfig} */
const nextConfig = {
  eslint: {
    ignoreDuringBuilds: true,
  },
  serverExternalPackages: ['pg'], // Changed from experimental.serverComponentsExternalPackages
};

module.exports = nextConfig;
