import pandas as pd
from config.db_config import DB_CONFIG
from etl.extract import extract_data, stage_data
from etl.transform.transform_products import transform_products
from etl.load import load_data
from etl.schema_evolution import apply_schema_evolution, alter_parquet_schema, get_current_schema, update_metadata
import datetime
import logging
from config.logging_config import logger

def main():
    logger.info("Starting ETL process")

    # Define paths and last extract date
    last_extract_date = DB_CONFIG['last_extract_date']
    raw_data_path = DB_CONFIG['raw_data_path'] + 'products.csv'
    staging_path = DB_CONFIG['staging_path'] + 'products.parquet'
    warehouse_path = DB_CONFIG['warehouse_path'] + 'product_dim.parquet'
    metadata_path = DB_CONFIG['metadata_path'] + 'products_metadata.json'

    logger.info("Extracting new or updated products data")
    # Extract new or updated products data
    new_products_data = extract_data(raw_data_path, last_extract_date)
    stage_data(new_products_data, staging_path)
    
    # Load existing dimension data
    try:
        existing_products_data = pd.read_parquet(warehouse_path)
    except FileNotFoundError:
        existing_products_data = pd.DataFrame()

    logger.info("Transforming new or updated products data")
    # Transform new or updated products data
    transformed_products_data = transform_products(new_products_data, existing_products_data)
    
    logger.info("Detecting schema changes and applying evolution")
    # Detect schema changes and apply evolution
    current_metadata = get_current_schema(metadata_path)
    existing_schema = {field['name']: field['type'] for field in current_metadata['fields']}
    new_schema = {
        "product_id": "int",
        "product_name": "string",
        "category": "string",
        "price": "float",
        "product_description": "string",  # New column
        "start_date": "date",
        "end_date": "date"
    }

    added_columns, removed_columns = apply_schema_evolution(existing_schema, new_schema)
    alter_parquet_schema(warehouse_path, added_columns)

    logger.info("Updating metadata with new schema")
    # Update metadata with new schema
    current_metadata['fields'].append({"name": "product_description", "type": "string"})
    update_metadata(current_metadata, metadata_path)
    
    logger.info("Loading transformed data into the data warehouse")
    # Load transformed data into the data warehouse
    load_data(transformed_products_data, warehouse_path)
    logger.info("ETL process completed successfully")

if __name__ == '__main__':
    main()
