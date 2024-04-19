# Dataforge Core

## Installation

- Download and install Python version 3.12+ from https://www.python.org/downloads/
- Install Dataforge by running: 
  - pip install dataforge-core
- Install beta 
  - pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ dataforge-core
  
## Usage
### Command
<code>dataforge [-h] [--init] [--seed] [--connect "Postgres connection string"] [Project Path]</code>


### Arguments:
  [Project Path]        Project folder. Optional, defaults to current folder

### Options:
  - -h, --help            show this help message and exit
  - --init, -i            Initialize project folder with sample files
  - --seed                Deploy and seed postgres database
  - --connect | -c "Postgres connection string" Connect to, deploy and initialize postgres database

## Links
- https://dataforgelabs.com