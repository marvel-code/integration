# Data Integration Tool

A Python-based data integration tool that processes and transforms data using various file formats and database connections.

## Pipeline

The data integration pipeline consists of several stages:

1. **Raw Data Processing** 🔄 in progress

   - Ingestion of data from various sources (Excel, Access DB)
   - Initial validation of data structure and content
   - Storage of raw data in a standardized format

2. **Data Cleansing** 📋 in future

   - Removal of duplicates
   - Standardization of data formats
   - Handling of missing values and inconsistencies

3. **Business Logic Processing** 📋 in future

   - Application of business rules and transformations
   - Data enrichment and aggregation
   - Quality checks and validations

4. **Output Generation** 📋 in future
   - Production of standardized output files
   - Generation of processing reports
   - Error logging and monitoring

The pipeline is designed to be modular and extensible, allowing for easy addition of new data sources and processing steps.

## Features

- Supports multiple file formats (Excel, Access DB)

## Prerequisites

### System Dependencies

#### macOS

```bash
# Install mdbtools for Access database support
brew install mdbtools

# Install Poetry for Python dependency management
curl -sSL https://install.python-poetry.org | python3 -
```

#### Windows

- Install [mdbtools](https://github.com/mdbtools/mdbtools/releases)
- Install [Poetry](https://python-poetry.org/docs/#installation)

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd integration
```

2. Install Python dependencies:

```bash
poetry install
```

## Usage

The tool processes data from an input directory and outputs the results to a specified output directory.

```bash
# Run the integration tool
sh run.sh
```

By default, the script processes files from `data/input` and saves results to `data/output`.

## Project Structure

```
.
├── data/
│   ├── input/     # Input data files
│   └── output/    # Processed output files
├── integration/   # Main package code
├── poetry.lock    # Dependency lock file
├── pyproject.toml # Project configuration
└── run.sh        # Execution script
```

## Dependencies

- Python 3.12+
- pandas
- openpyxl
- xlrd
- xlwt
- pyodbc
- msaccessdb
- requests

# Plans

## Raw module

- [x] MDB adapter fetches all tables from mdb file.
- [x] Source tables saved to raw/ with structure in input.
- [x] Add extension to the end of output file like "<filename>\_<orig_ext>.xlsx".
- [ ] Add table name for the file (like mdb) like "<orig*filename>\_<orig_tablename>*<orig_ext>.xlsx"
- [ ] Output to the folder of launch (e.g. output/YYYY-MM-DD_HH_mm_ss/raw/...)
- [ ]
- [ ]
- [ ]
- [ ]
