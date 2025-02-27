# Automated Testing

## Important Folders
cypress/e2e/NewTests contains all tests and logic for the current IDO test suite
cypress/e2e/fixtures contains static files for use in tests
cypress/plugins contains commands that leverage external objects like postgres, snowflake, sql server, etc.
cypress/support contains the general_commands.js file used to keep commonly repeated logic

## Important Files
cypress.env.json and cypress.envTemplate.json are used to track values that should change between each IDO environment
Dockerfile defines the docker image that will be created for each push of code. These docker images will run in ECS to execute our automated tests
runTests.sh is the script that will run on the docker image when running tests. It contains multiple ordered cypress commands that run our tests in order with respect to test dependencies
bitbucket-pipelines.yml defines the steps taken in bitbucket whenever code is pushed to the repo. It will do all necessary steps to build a new docker image and run it in AWS ECS.
See https://miro.com/app/board/uXjVPJ1R1BM=/?share_link_id=115452209837 for diagram detailing the bitbucket pipeline and running of these tests.


This folder contains simple calls to the functions created in the Runners folder to execute tests
Subfolders group tests of the same kind. i.e. Source creation tests
Each file will import functions from the Runners folder
Each file should first read in the name of the file. This will be passed as the argument to the Runner function
These files should contain no action steps or assertions. They should only contains calls to functions defined in the "Runners" folder