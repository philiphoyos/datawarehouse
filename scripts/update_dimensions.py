# from etl.update_dimensions import update_dimensions

# if __name__ == '__main__':
#     source = 'path/to/new_data.csv'
#     destination = 'path/to/product_dim.parquet'
#     update_dimensions(source, destination)

import pandas as pd
import os
from config.db_config import DB_CONFIG
from etl.extract import extract_data, stage_data
from etl.transform.transform_products import transform_products
from etl.transform.transform_customers import transform_customers
from etl.load import load_data
from etl.schema_evolution import apply_schema_evolution, alter_parquet_schema, get_current_schema, update_metadata
import datetime
import logging
from config.logging_config import logger

def update_dimension(dimension_name, transform_func, metadata_file, raw_file, staging_file, warehouse_file):
    logger.info(f"Starting update for {dimension_name} dimension")

    # Extract new or updated data
    last_extract_date = DB_CONFIG['last_extract_date']
    new_data = extract_data(raw_file, last_extract_date)
    stage_data(new_data, staging_file)

    # Load existing dimension data
    try:
        existing_data = pd.read_parquet(warehouse_file)
    except FileNotFoundError:
        existing_data = pd.DataFrame()

    # Transform new or updated data
    transformed_data = transform_func(new_data, existing_data)

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

    logger.info(f"Update for {dimension_name} dimension completed successfully")

def main():
    # Product Dimension
    update_dimension(
        dimension_name="product",
        transform_func=transform_products,
        metadata_file='metadata/products_metadata.json',
        raw_file='raw_data/products.csv',
        staging_file='staging/products.parquet',
        warehouse_file='warehouse/product_dim.parquet'
    )

    # Customer Dimension
    update_dimension(
        dimension_name="customer",
        transform_func=transform_customers,
        metadata_file='metadata/customers_metadata.json',
        raw_file='raw_data/customers.csv',
        staging_file='staging/customers.parquet',
        warehouse_file='warehouse/customer_dim.parquet'
    )

if __name__ == '__main__':
    main()
