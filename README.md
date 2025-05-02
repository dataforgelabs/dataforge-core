![DataForge Core-Light](etc/DataForge_Core_Flow.svg#gh-light-mode-only)
![DataForge Core-Dark](etc/DataForge_Core_Flow_Reverse.svg#gh-dark-mode-only)

[DataForge](https://www.dataforgelabs.com) helps data analysts and engineers build and extend data solutions by leveraging modern software engineering principles.

[![OSSRank](https://shields.io/endpoint?url=https://ossrank.com/shield/4025)](https://ossrank.com/p/4025)
[![Upload Python Package](https://github.com/dataforgelabs/dataforge-core/actions/workflows/python-publish.yml/badge.svg)](https://github.com/dataforgelabs/dataforge-core/actions/workflows/python-publish.yml)

## Understanding DataForge

DataForge enables writing of inline functions using single-column SQL expressions rather than CTEs, procedural scripts, or set-based models.

For an overview of the underlying concepts, check out this introduction [blog](https://www.dataforgelabs.com/blog/introduction-dataforge-framework).

Each function:
- is [pure](https://en.wikipedia.org/wiki/Pure_function), with no [side effects](https://en.wikipedia.org/wiki/Side_effect_(computer_science))
- returns single column
- is composable with other functions

DataForge software engineering principles:
- [Functional Programming](https://en.wikipedia.org/wiki/Functional_programming)
- [Inversion of Control](https://en.wikipedia.org/wiki/Inversion_of_control)
- [Single Responsibility Principal](https://en.wikipedia.org/wiki/Single-responsibility_principle)
- [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself)

These principles allow DataForge projects to be easy to modify and extend - even with thousands of integrated pipelines.

Explore the Core CLI or [learn more](https://www.dataforgelabs.com/core-vs-cloud) about how Core powers DataForge Cloud.

## Requirements
Dataforge Core is a code framework and command line tool to develop transformation functions and compile them into executable Spark SQL.

To run the CLI you will need:
- Java 8 or higher
  - [Amazon Corretto](https://docs.aws.amazon.com/corretto/) is a great option
- A PostgreSQL v14+ server with a dedicated empty database
  - Check out our friends over at [Tembo](https://tembo.io/)
- Python version 3.12+
  - [Official Link](https://www.python.org/downloads/)

The CLI also includes an integration to run the code in Databricks. To support this you need:
- [Databricks Workspace](https://docs.databricks.com/en/administration-guide/workspace/index.html)
- [Databricks SQL Warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html)
- [Developer Personal Access Token](https://docs.databricks.com/en/dev-tools/auth/pat.html)



## Installation and Quickstart

- Open a new command line window
- Validate Java and Python are installed correctly:

  ```
  > java --version
  openjdk 21.0.3 2024-04-16 LTS
  ```
  ```
  > python --version
  Python 3.12.3
  ```
- Install Dataforge by running: 
  ```
  > pip install dataforge-core
  Collecting dataforge-core...
  Installing collected packages: dataforge-core
  Successfully installed dataforge-core...
  ```
- Validate installation:
  ```
  > dataforge --version
  dataforge-core 1.0.0
  ```
- Configure connections and credentials to Postgres and optionally Databricks
  ```
  > dataforge --configure
  Enter postgres connection string: postgresql://postgres:<postgres-server-url>:5432/postgres
  Do you want to configure Databricks SQL Warehouse connection (y/n)? y
  Enter Server hostname: <workspace-url>.cloud.databricks.com
  Enter HTTP path: /sql/1.0/warehouses/<warehouse-guid>
  Enter access token: <token-guid>
  Enter catalog name: <unity_catalog_name>
  Enter schema name: <schema_in_catalog_name>
  Connecting to Databricks SQL Warehouse <workspace-url>.cloud.databricks.com
  Databricks connection validated successfully
  Profile saved in C:\Users...
  ```

- Navigate to an empty folder and initialize project structure and sample files:
  ```
  > dataforge --init
  Initialized project in C:\Users...
  ```

- Deploy dataforge structures to Postgres
  ```
  > dataforge --seed
  All objects in schema(s) log,meta in postgres database will be deleted. Do you want to continue (y/n)? y
  Initializing database..
  Database initialized
  ```
- Build sample project
  ```
  > dataforge --build
  Validating project path C:\Users...
  Started import with id 1
  Importing project files...
  <list of files>
  Files parsed
  Loading objects...
  Objects loaded
  Expressions validated
  Generated 8 source queries
  Generated 1 output queries
  Generated run.sql
  Import completed successfully
  ```
- Execute in Databricks
  ```
  > dataforge --run
  Connecting to Databricks SQL Warehouse <workspace-url>.cloud.databricks.com
  Executing query
  Execution completed successfully
  ```
## Commands

  <table>
  <tr><td>-h, --help</td><td>Display this help message and exit</td></tr>
  <tr><td>-v, --version</td><td>Display the installed DataForge version</td></tr>
  <tr><td>-c, --configure</td><td>Connect to Postgres database and optionally Databricks SQL Warehouse</td></tr>
  <tr><td>-s, --seed</td><td>Deploy tables and scripts to postgres database</td></tr>
  <tr><td>-i, --init [Project Path]</td><td>Initialize project folder structure with sample code</td></tr>
  <tr><td>-b, --build [Project Path]</td><td>Compile code, store results in Postgres, and generate target SQL files</td></tr>
  <tr><td>-r, --run [Project Path]</td><td>Run compiled project on Databricks SQL Warehouse</td></tr>
  <tr><td>-p, --profile [Profile Path]</td><td>Update path of stored credentials profile file</td></tr>
 </table>

## Links
- https://dataforgelabs.com
