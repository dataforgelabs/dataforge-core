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

    runTerminalCommand(`pip install -i https://test.pypi.org/simple/ dataforge-core`).then((output) => {
      cy.log(JSON.stringify(output))
    });

    runTerminalCommand('dataforge --version').then((output) => {
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

    runTerminalCommand('dataforge --init').then((output) => {
      cy.log(JSON.stringify(output))
    });

    cy.exec('node scripts/runInteractiveCommand.js --seed', { failOnNonZeroExit: true }).then((result) => {
      expect(result.stdout).to.include('Process ended with 0')
    });

    runTerminalCommand('dataforge --build').then((output) => {
      cy.log(JSON.stringify(output))
    });

    runTerminalCommand('dataforge --run').then((output) => {
      cy.log(JSON.stringify(output))
    });
  });
});
