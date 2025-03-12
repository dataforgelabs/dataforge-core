// ***********************************************************
// This example plugins/index.js can be used to load plugins
//
// You can change the location of this file or turn off loading
// the plugins file with the 'pluginsFile' configuration option.
//
// You can read more here:
// https://on.cypress.io/plugins-guide
// ***********************************************************

// This function is called when a project is opened or re-opened (e.g. due to
// the project's config changing)
//New commit test

const common_params = require('../cypress.env.json')

const login_creds = (user_type) => {
  return credentials[user_type]
}

const auth0_creds = () => {
  return credentials["auth0_creds"]
}

module.exports = (on, config) => {
  // `on` is used to hook into various events Cypress emits
  // `config` is the resolved Cypress config
  on('before:browser:launch', (browser = {}, launchOptions) => {
    // browser will look something like this
    // {
    //   name: 'chrome',
    //   displayName: 'Chrome',
    //   version: '63.0.3239.108',
    //   path: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    //   majorVersion: '63'
    // }

    if (browser.name === 'chrome') {
      // `args` is an araay of all the arguments
      // that will be passed to Chrome when it launchers
      launchOptions.args.push('--disable-web-security')

      // whatever you return here becomes the new args
      return launchOptions
    }
  })

  on('task', {
    'securePassword'(testName) {
      return credentials["passwords"][testName] ? credentials["passwords"][testName] : "error"

    }
  })

  on('task', {
    'login_creds'(user_type) {
      return login_creds(user_type)
    }
  })

  on('task', {
    'auth0_creds'() {
      return auth0_creds()
    }
  })
}