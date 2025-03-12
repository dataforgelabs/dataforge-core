const { spawn } = require('child_process');

const args = process.argv.slice(2);
const commandArgs = args.length > 0 ? args : ['--configure'];
const commandProcess = spawn('dataforge', [commandArgs]);

const postgresConnectionString = process.env.POSTGRES_CONNECTION_STRING;
const databricksHostname = process.env.DATABRICKS_HOSTNAME;
const databricksHttpPath = process.env.DATABRICKS_HTTP_PATH;
const databricksAccessToken = process.env.DATABRICKS_ACCESS_TOKEN;

console.log('Postgres Connection String:', postgresConnectionString);
console.log('Databricks Hostname:', databricksHostname);
console.log('Databricks HTTP Path:', databricksHttpPath);
console.log('Databricks Access Token:', databricksAccessToken);

let output = '';

const configureResponses = [
  { prompt: 'Enter Postgres connection string:', answer: postgresConnectionString },
  { prompt: 'Do you want to configure Databricks SQL Warehouse connection (y/n)?', answer: 'y' },
  { prompt: 'Enter Server hostname:', answer: databricksHostname },
  { prompt: 'Enter HTTP path:', answer: databricksHttpPath },
  { prompt: 'Enter access token:', answer: databricksAccessToken },
  { prompt: 'Enter catalog name:', answer: 'cypress_open_source' },
  { prompt: 'Enter schema name:', answer: 'aw' }
];


const seedResponses = [
  { prompt: 'All objects in schema(s) log,meta in postgres database will be deleted. Do you want to continue (y/n)?', answer: 'y' }
]

//Select the prompt deppending on the command
const selectedResponses = commandArgs[0].includes('seed') ? seedResponses : configureResponses;

//Process the output terminal
commandProcess.stdout.on('data', (data) => {
  output += data.toString();
  console.log('Terminal output: ', data.toString());

  // Verify in a prompt exists on the process output
  selectedResponses.forEach((response) => {    
    if (output.includes(response.prompt)) {
      console.log(`Responding to the prompt: "${response.prompt}" with "${response.answer}"`);
      commandProcess.stdin.write(`${response.answer}\n`);
      selectedResponses.shift() //Remove the answered process
      output = ''; // Clean the output
    }
  });
});

// Log errors
commandProcess.stderr.on('data', (data) => {
  console.error('Error', data.toString());
});

// Log process code
commandProcess.on('close', (code) => {
  console.log(`Process ended with ${code}`);
});