const { defineConfig } = require('@vue/cli-service')

module.exports = defineConfig({
  // Enable hot module replacement
  devServer: {
    port: 4000,
    hot: true,
    liveReload: true,
    compress: true,
    open: true,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, PATCH, OPTIONS',
      'Access-Control-Allow-Headers': 'X-Requested-With, content-type, Authorization, X-Socket-Id',
      'Access-Control-Allow-Credentials': 'true'
    },
    proxy: {
      '^/api': {
        target: 'http://localhost:3000',
        changeOrigin: true,
        secure: false,
        ws: true,
        logLevel: 'debug'
      }
    },
    client: {
      overlay: {
        warnings: true,
        errors: true
      },
      progress: true,
      reconnect: 5
    },
    historyApiFallback: true
  },
  
  // Webpack configuration
  configureWebpack: {
    devtool: 'source-map',
    // Enable fast refresh
    devServer: {
      hot: true
    },
    // Performance hints are now configured in package.json
  },
  
  // CSS configuration
  css: {
    sourceMap: process.env.NODE_ENV !== 'production',
    loaderOptions: {
      // Add any CSS loader options here
    }
  },
  
  // Enable parallel processing
  parallel: require('os').cpus().length > 1,
  
  // Production source maps
  productionSourceMap: true
})
