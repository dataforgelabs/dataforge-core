![DataForge Core-Light](etc/DataForge_Core_Flow.svg#gh-light-mode-only)
![DataForge Core-Dark](etc/DataForge_Core_Flow_Reverse.svg#gh-dark-mode-only)

[DataForge](https://www.dataforgelabs.com) helps data analysts and engineers build and extend data solutions by leveraging modern software engineering principals.

## Understanding DataForge

DataForge enables writing of inline functions using single-column SQL expressions rather than CTEs, procedural scripts, or set-based models.


Each function:
- is [pure](https://en.wikipedia.org/wiki/Pure_function), with no [side effects](https://en.wikipedia.org/wiki/Side_effect_(computer_science))
- returns single column
- is composable with other functions

The software engineering principals Dataforge enables are:
- [Functional Programming](https://en.wikipedia.org/wiki/Functional_programming)
- [Inversion of Control](https://en.wikipedia.org/wiki/Inversion_of_control)
- [Single Responsibility Principal](https://en.wikipedia.org/wiki/Single-responsibility_principle)
- [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself)

These principals allow DataForge code to be hyper-extensible and easy to modify - even with thousands of integrated pipelines.



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
