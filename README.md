# Dataforge Core
## Description and motivation

Dataforge core enables declarative, functional programming paradigm in data engineering at the most granular, columnar level.
Developers write inline functions using SQL column expression syntax. Each function :
- returns single column (cell) value
- is implicitly parametrized on the components (column) used in the expression
- is pure, with no side effects
- is composable: functions can be chained and re-used
Dataforge compiler automatically tracks and resolves all dependencies between functions, enabling developers to focus on business logic

## Installation

- Download and install Python version 3.12+ from https://www.python.org/downloads/
- Install Dataforge by running: 
  - pip install dataforge-core
- Install latest beta release: 
  - pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ dataforge-core
  
## Usage
### Command
<code>dataforge [-h] [--build [Project Path]] [--init [Project Path]] [--seed] [--configure] [--ver]
                 [--profile "Dataforge profile file path"] [--run [Project Path]]</code>


### Arguments:
  [Project Path]        Project folder. Optional, defaults to current folder

  <table>
  <tr><td>-h, --help</td><td>show this help message and exit</td></tr>
  <tr><td>--configure</td><td>Connect to, and initialize postgres database and, optionally, Databricks SQL Warehouse</td></tr>
  <tr><td>--init [Project Path]</td><td>Initialize project folder structure with sample files</td></tr>
  <tr><td>--build [Project Path]</td><td>Build project</td></tr>
  <tr><td>--seed</td><td>Deploy and seed postgres database</td></tr>
  <tr><td>--profile</td><td> "Databricks SQL warehouse http path" Databricks SQL warehouse http path</td></tr>
  <tr><td>--run [Project Path]</td><td>Execute compiled project using configured Databricks SQL warehouse connection</td></tr>
 </table>

## Syntax
Run dataforge --init or check out dataforge/resources/project folder for project structure and syntax 

## Links
- https://dataforgelabs.com