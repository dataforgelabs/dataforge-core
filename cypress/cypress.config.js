const { defineConfig } = require('cypress')
const common_params = require('./cypress.env.json')

module.exports = defineConfig({
  viewportWidth: 1800,
  viewportHeight: 1000,
  watchForFileChanges: false,
  projectId: 'hkoa5o',
  video: false,
  taskTimeout: 500000,
  execTimeout: 600000,
  chromeWebSecurity: false,
  e2e: {
    // We've imported your old cypress plugins here.
    // You may want to clean this up later by importing these.
    setupNodeEvents(on, config) {
      require('./plugins/index')(on, config);
      return config;
    },
    specPattern: './e2e/Tests/*.js',
    supportFile: false,
    baseUrl: common_params.url,
    testIsolation: false},
})
