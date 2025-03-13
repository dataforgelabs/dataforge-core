/// <reference types="Cypress" />
const envFile = require('../../cypress.env.json')

const runTerminalCommand = (command) => {
  return cy.exec(command).then((result) => {
    if (result.code !== 0) {
      throw new Error(`Error running command: ${command}\n${result.stderr}`);
    }
    return result;
  });
};

describe('Test Dataforge Open Source', () => {
  it('Execute comand lines', () => {

    cy.exec('java -version').then((output) => {
      cy.log(JSON.stringify(output.stderr))
    });

    runTerminalCommand('python --version').then((output) => {
      cy.log(JSON.stringify(output))
    });

    cy.exec('node scripts/runInteractiveCommand.js --configure', {
      failOnNonZeroExit: true,
      env: {
        POSTGRES_CONNECTION_STRING: process.env.POSTGRES_CONNECTION_STRING,
        DATABRICKS_HOSTNAME: process.env.DATABRICKS_HOSTNAME,
        DATABRICKS_HTTP_PATH: process.env.DATABRICKS_HTTP_PATH,
        DATABRICKS_ACCESS_TOKEN: process.env.DATABRICKS_ACCESS_TOKEN
      }
    }).then((result) => {
      expect(result.stdout).to.include('Process ended with 0')
      expect(result.stdout).to.not.include('Databricks connection validated successfully Profile saved')
    });

    runTerminalCommand('dataforge --version').then((output) => {
      cy.log(JSON.stringify(output))
    });

    runTerminalCommand('dataforge --init').then((output) => {
      cy.log(JSON.stringify(output))
    });

    cy.exec('node scripts/runInteractiveCommand.js --seed', { failOnNonZeroExit: true }).then((result) => {
      expect(result.stdout).to.include('Process ended with 0')
    });

    runTerminalCommand('dataforge --build').then((output) => {
      cy.log('Full build output: ' + JSON.stringify(output));
    });

    runTerminalCommand('dataforge --run').then((output) => {
      cy.log(JSON.stringify(output))
    });
  });
});
