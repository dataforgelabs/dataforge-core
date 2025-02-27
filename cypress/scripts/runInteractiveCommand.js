const { spawn } = require('child_process');

const args = process.argv.slice(2);
const commandArgs = args.length > 0 ? args : ['--configure'];
const commandProcess = spawn('dataforge', [commandArgs]);
const token = process.env.DATABRICKS_TOKEN;

let output = '';

const configureResponses = [
  { prompt: 'Enter Postgres connection string:', answer: 'postgresql://postgres:0QdwaZiQXEZCsrwX@lastly-beautiful-sandpiper.data-1.use1.tembo.io:5432/postgres' },
  { prompt: 'Do you want to configure Databricks SQL Warehouse connection (y/n)?', answer: 'y' },
  { prompt: 'Enter Server hostname:', answer: 'https://dbx-dataforge-next.cloud.databricks.com' },
  { prompt: 'Enter HTTP path:', answer: '/sql/1.0/warehouses/ddbee00edc06499d' },
  { prompt: 'Enter access token:', answer: token },
  { prompt: 'Enter catalog name:', answer: 'cypress_open_source' },
  { prompt: 'Enter schema name:', answer: 'aw' }
]

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