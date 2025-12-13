import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  webpack: (config, { isServer }) => {
    if (isServer) {
      config.externals = config.externals || [];
      config.externals.push({
        duckdb: "commonjs duckdb",
        "@mapbox/node-pre-gyp": "commonjs @mapbox/node-pre-gyp",
      });
    }
    return config;
  },
};

export default nextConfig;
