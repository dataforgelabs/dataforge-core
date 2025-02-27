Cypress.Commands.add('runTerminalCommand', (command) => {
  return cy.exec(command).then((result) => {
    if (result.code !== 0) {
      throw new Error(`Errr running comand: ${command}\n${result.stderr}`);
    }
    return result;
  });
});
