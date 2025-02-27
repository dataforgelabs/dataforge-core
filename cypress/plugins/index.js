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
//aws sdk
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager'),
  { S3Client, CopyObjectCommand, GetObjectCommand, DeleteObjectCommand, SelectObjectContentCommand, ListObjectsV2Command } = require("@aws-sdk/client-s3")
region = common_params.region,
  secretName = common_params.env_name + "/cypress";

const s3Client = new S3Client({ region: region })

async function getCredentials(credentialType) {
  var secretsClient = new SecretsManagerClient({
    region: region
  });

  const getSecretCommand = new GetSecretValueCommand({ SecretId: secretName })
  return new Promise((resolve, reject) => {
    secretsClient.send(getSecretCommand).then(res => {
      // Decrypts secret using the associated KMS CMK.
      // Depending on whether the secret is a string or binary, one of these fields will be populated.
      if ('SecretString' in res) {
        resolve(JSON.parse(res.SecretString)[credentialType]);
      } else {
        let buff = Buffer.from(res.SecretBinary, 'base64');
        resolve(buff.toString('ascii'));
      }
    }).catch(ex => {
      console.error("Unable to read DB credentials from AWS : " + ex);
    });
  });
}

var credentials;

const secretsClient = new SecretsManagerClient({
  region: region
});
const getSecretCommand = new GetSecretValueCommand({ SecretId: secretName })

secretsClient.send(getSecretCommand).then(res => {
  console.log('CREDS loaded from secret')
  credentials = JSON.parse(res.SecretString)
}).catch(ex => {
  console.error("Unable to read credentials from AWS secret: " + ex + " stack trace: " + ex.stack);
});

// wait 2 sec to give credentials time to load
Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 2000);

// SQL Server query
//THIS IS LIKELY BROKEN DUE TO LIBRARY UPDATES! PLEASE REVERIFY IF you PLAN TO USE IT
const mssql = require('mssql')
const sql = query => {
  creds = credentials["sql_connection"]
  return new Promise(
    function (resolve, reject) {

      mssql.connect(creds)
        .then(() => { return mssql.query(query) })
        .then(res => {
          mssql.close()
          if (res.recordset)
            resolve(res.recordset)
          else
            resolve(null)
        })
        .catch(err => {
          console.error('SQL Server Query error: ', err)
          mssql.close()
          reject(err)
        })
    })
}

// Postgres query
const { Client } = require('pg')

const pg = (query, db) => {
  const pg_conn = credentials["pg_connection"]
  if (db == 'output')
    pg_conn.database = db

  const client = new Client({
    user: pg_conn.user,
    password: pg_conn.password,
    database: pg_conn.database,
    port: pg_conn.port,
    host: pg_conn.host,
    application_name: "RTE_testing",
    ssl: 'no-verify'
  })

  return client.connect()
    .then(() => {
      return client.query(query).then(res => {

        client.end()
        return res
      }).catch(err => {
        console.error('Postgres connection error', err)
        client.end()
        return err
      })
    })
    .catch(err => {
      console.error('Postgres connection error', err.stack)
      client.end()
      return err
    })

}


delay = function (t) {
  return new Promise(function (resolve) {
    setTimeout(resolve, t)
  })
}

// Postgres retry query
function retry(fn, query, retries = 10, ms = 1000, err = null) {
  if (!retries) {
    console.log('# of retries exhausted')
    return Promise.resolve({ error: "timeout" });
  }
  else {
    return fn(query).then(res => {
      if (res.rowCount) {
        console.log('resolved!')
        return Promise.resolve(res)
      }
      else {
        console.log('retry ' + retries)
        return delay(ms).then(() => { return retry(fn, query, (retries - 1), ms, err) })

      }
    })
      .catch(err => {
        return Promise.reject(err)
      });
  }

}


// Snowflake query
const snowflake = require('snowflake-sdk')

const sf = query => {
  creds = credentials["snowflake_connection"]
  const snowflake_connection = snowflake.createConnection(creds)

  snowflake_connection.connect(
    function (err, conn) {
      if (err) {
        console.error('Unable to connect to Snowflake: ' + err.message);
      }
      else {
        // console.log('Successfully connected to Snowflake.');
      }
    }
  )

  return new Promise(
    function (resolve, reject) {
      snowflake_connection.execute({
        sqlText: query,
        complete: function (err, stmt, rows) {
          if (err) {
            console.error('Failed to execute statement due to the following error: ' + err.message);
            snowflake_connection.destroy(() => { })
            reject(err)
          } else {
            // console.log('Number of rows produced: ' + rows);
            snowflake_connection.destroy(() => { })
            resolve(rows)
          }
        }
      })
    })
}

const login_creds = (user_type) => {
  return credentials[user_type]
}

const secure_passwords = () => {
  return credentials["passwords"]
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
    'privateKey'(testName) {
      return credentials["privateKey"][testName] ? credentials["privateKey"][testName] : "error"

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

  on('task', {
    'databricks_access_token'(testName) {
      return credentials['databricks_access_token'][testName]
    }
  })
}