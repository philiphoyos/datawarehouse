# Data Warehouse ETL Scripts

## Overview
This repository contains ETL scripts for updating dimensions and facts in a data warehouse.

## Directory Structure
- `etl/`: Contains modular ETL scripts for data extraction, transformation, and loading.
- `scripts/`: Contains high-level scripts for running specific ETL tasks.
- `config/`: Contains configuration files for database connections and logging.
- `tests/`: Contains test scripts for unit testing the ETL functions.

## Usage
1. **Initial Load**:
    ```sh
    python scripts/initial_load.py
    ```

2. **Update Dimensions**:
    ```sh
    python scripts/update_dimensions.py
    ```

3. **Update Facts**:
    ```sh
    python scripts/update_facts.py
    ```

## Configuration
- Update database connection settings in `config/db_config.py`.
- Update logging settings in `config/logging_config.py`.

## Testing
Run tests using:
```sh
pytest tests/
