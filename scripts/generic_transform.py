import pandas as pd
import os
import shutil
from config.db_config import DB_CONFIG
from etl.extract import extract_data, stage_data
from etl.transform import transform_data
from etl.load import load_data
from etl.schema_evolution import apply_schema_evolution, alter_parquet_schema, get_current_schema, update_metadata
import datetime
import logging
from config.logging_config import logger

def move_to_archive(file_path, archive_dir):
    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)
    shutil.move(file_path, os.path.join(archive_dir, os.path.basename(file_path)))

def generic_transform(dimension_name, metadata_file, raw_file, staging_file, warehouse_file, archive_dir):
    logger.info(f"Starting update for {dimension_name}")

    # Extract new or updated data
    last_extract_date = DB_CONFIG['last_extract_date']
    new_data = extract_data(raw_file, last_extract_date)
    stage_data(new_data, staging_file)

    # Load existing data
    try:
        existing_data = pd.read_parquet(warehouse_file)
    except FileNotFoundError:
        existing_data = pd.DataFrame()

    # Transform new or updated data
    transformed_data = transform_data(new_data, existing_data)

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

    # Load transformed data into the data warehouse
    load_data(transformed_data, warehouse_file)

    # Move raw data to archive
    move_to_archive(raw_file, archive_dir)

    logger.info(f"Update for {dimension_name} completed successfully")

def main():
    # Generic transform for a dimension
    generic_transform(
        dimension_name="generic",
        metadata_file='metadata/generic_metadata.json',
        raw_file='raw_data/data_source.csv',
        staging_file='staging/data_source.parquet',
        warehouse_file='warehouse/data_source.parquet',
        archive_dir='archived_data/data_source'
    )

if __name__ == '__main__':
    main()
