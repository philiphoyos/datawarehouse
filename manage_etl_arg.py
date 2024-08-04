import argparse
import pandas as pd
import os
import shutil
import json
from config.db_config import DB_CONFIG
from etl.extract import extract_data, stage_data
from etl.transform import transform_data
from etl.load import load_data
from etl.schema_evolution import apply_schema_evolution, alter_parquet_schema, get_current_schema, update_metadata
import logging
from config.logging_config import logger

def move_to_archive(file_path, archive_dir):
    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)
    shutil.move(file_path, os.path.join(archive_dir, os.path.basename(file_path)))

def update_metadata_registry(metadata_file, common_metadata_file, dimension_name):
    with open(metadata_file, 'r') as file:
        new_metadata = json.load(file)
    
    if os.path.exists(common_metadata_file):
        with open(common_metadata_file, 'r') as file:
            common_metadata = json.load(file)
    else:
        common_metadata = {}
    
    common_metadata[dimension_name] = new_metadata
    with open(common_metadata_file, 'w') as file:
        json.dump(common_metadata, file, indent=4)

def process_data(dimension_name, metadata_file, raw_file, staging_file, warehouse_file, archive_dir, process_known):
    logger.info(f"Processing {dimension_name}")

    # Extract new or updated data
    if process_known:
        new_data = pd.read_csv(raw_file)
    else:
        last_extract_date = DB_CONFIG['last_extract_date']
        new_data = extract_data('csv', None,raw_file, last_extract_date)

    stage_data(new_data, staging_file)

    # Load existing data
    try:
        existing_data = pd.read_parquet(warehouse_file)
    except FileNotFoundError:
        existing_data = pd.DataFrame()

    # Transform new or updated data
    transformed_data = transform_data(new_data, '' , existing_data)

    # Detect schema changes and apply evolution
    current_metadata = get_current_schema(metadata_file)
    existing_schema = {field['name']: field['type'] for field in current_metadata['fields']}
    
    # New schema derived from the transformed data
    new_schema = {col: str(transformed_data[col].dtype) for col in transformed_data.columns}

    added_columns, removed_columns = apply_schema_evolution(existing_schema, new_schema)
    alter_parquet_schema(warehouse_file, added_columns)

    # Update metadata with new schema
    for column in added_columns:
        current_metadata['fields'].append({"name": column, "type": str(transformed_data[column].dtype)})
    update_metadata(current_metadata, metadata_file)

    # Update common metadata registry
    update_metadata_registry(metadata_file, os.path.join(DB_CONFIG['metadata_path'], 'common_metadata.json'), dimension_name)

    # Load transformed data into the data warehouse
    load_data(transformed_data, warehouse_file)

    # Move raw data to archive
    move_to_archive(raw_file, archive_dir)

    logger.info(f"Processing for {dimension_name} completed successfully")

def main():
    parser = argparse.ArgumentParser(description="ETL Script for Data Transformation")
    parser.add_argument('--dimension_name', required=True, help="Name of the dimension to process")
    parser.add_argument('--raw_file', required=True, help="Path to the raw data file")
    parser.add_argument('--process_known', action='store_true', help="Process known data from raw_data directory")

    args = parser.parse_args()

    dimension_name = args.dimension_name
    raw_file = args.raw_file
    process_known = args.process_known

    metadata_file = f'metadata/{dimension_name}_metadata.json'
    staging_file = f'staging/{dimension_name}.parquet'
    warehouse_file = f'warehouse/{dimension_name}.parquet'
    archive_dir = f'archived_data/{dimension_name}'

    process_data(dimension_name, metadata_file, raw_file, staging_file, warehouse_file, archive_dir, process_known)

if __name__ == '__main__':
    main()
