/// <reference types="Cypress" />
const envFile = require('../../cypress.env.json')

describe('Test Dataforge Open Source', () => {
  it('Execute comand lines', () => {

    cy.exec('java -version').then((output) => {
      cy.log(JSON.stringify(output.stderr))
    });

    cy.runTerminalCommand('python --version').then((output) => {
      cy.log(JSON.stringify(output))
    });

    cy.runTerminalCommand(`pip install -i https://test.pypi.org/simple/ dataforge-core==${envFile.dataforge_core_rc_version}`).then((output) => {
      cy.log(JSON.stringify(output))
    });

    cy.runTerminalCommand('dataforge --version').then((output) => {
      cy.log(JSON.stringify(output))
    });

    cy.task('databricks_access_token', 'CypressOpenSource').then(accessToken => {
      cy.exec('node scripts/runInteractiveCommand.js --configure', {
        failOnNonZeroExit: true,
        env: {
          ...process.env,
          DATABRICKS_TOKEN: accessToken
        }
      }).then((result) => {
        expect(result.stdout).to.include('Process ended with 0')
        expect(result.stdout).to.not.include('Databricks connection validated successfully Profile saved')
      });
    })

    cy.runTerminalCommand('dataforge --init').then((output) => {
      cy.log(JSON.stringify(output))
    });

    cy.exec('node scripts/runInteractiveCommand.js --seed', { failOnNonZeroExit: true }).then((result) => {
      expect(result.stdout).to.include('Process ended with 0')
    });

    cy.runTerminalCommand('dataforge --build').then((output) => {
      cy.log(JSON.stringify(output))
    });

    cy.runTerminalCommand('dataforge --run').then((output) => {
      cy.log(JSON.stringify(output))
    });
  });
});
