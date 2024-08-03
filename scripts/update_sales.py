# import pandas as pd
# from etl.extract import extract_data
# from etl.transform.transform_sales import transform_sales
# from etl.load import load_data

# if __name__ == '__main__':
#     # Read staged data
#     sales_data = pd.read_parquet('staging/sales.parquet')
#     product_data = pd.read_parquet('warehouse/product_dim.parquet')
#     customer_data = pd.read_parquet('warehouse/customer_dim.parquet')
    
#     # Transform data
#     transformed_data = transform_sales(sales_data, product_data, customer_data)
    
#     # Load transformed data into the data warehouse
#     load_data(transformed_data, 'warehouse/sales_fact.parquet')

# import pandas as pd
# from etl.extract import extract_data, stage_data
# from etl.transform.transform_sales import transform_sales
# from etl.load import load_data
# import datetime

# if __name__ == '__main__':
#     # Define the last extraction date (this would typically come from a metadata store or config)
#     last_extract_date = datetime.datetime(2023, 6, 2)
    
#     # Extract new or updated sales data
#     new_sales_data = extract_data('raw_data/sales.csv', last_extract_date)
#     stage_data(new_sales_data, 'staging/sales.parquet')
    
#     # Load existing fact data
#     try:
#         existing_sales_data = pd.read_parquet('warehouse/sales_fact.parquet')
#     except FileNotFoundError:
#         existing_sales_data = pd.DataFrame()
    
#     # Load product and customer dimensions to ensure foreign keys are correctly handled
#     product_data = pd.read_parquet('warehouse/product_dim.parquet')
#     customer_data = pd.read_parquet('warehouse/customer_dim.parquet')
    
#     # Transform new or updated sales data
#     transformed_sales_data = transform_sales(new_sales_data, product_data, customer_data)
    
#     # Load transformed data into the data warehouse
#     load_data(transformed_sales_data, 'warehouse/sales_fact.parquet')

import pandas as pd
import os
from config.db_config import DB_CONFIG
from etl.extract import extract_data, stage_data
from etl.transform.transform_sales import transform_sales
from etl.load import load_data
from etl.schema_evolution import apply_schema_evolution, alter_parquet_schema, get_current_schema, update_metadata
import datetime
import logging
from config.logging_config import logger

def update_fact(fact_name, transform_func, metadata_file, raw_file, staging_file, warehouse_file):
    logger.info(f"Starting update for {fact_name} fact table")

    # Extract new or updated data
    last_extract_date = DB_CONFIG['last_extract_date']
    new_data = extract_data(raw_file, last_extract_date)
    stage_data(new_data, staging_file)

    # Load existing fact data
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

    logger.info(f"Update for {fact_name} fact table completed successfully")

def main():
    # Sales Fact Table
    update_fact(
        fact_name="sales",
        transform_func=transform_sales,
        metadata_file='metadata/sales_metadata.json',
        raw_file='raw_data/sales.csv',
        staging_file='staging/sales.parquet',
        warehouse_file='warehouse/sales_fact.parquet'
    )

if __name__ == '__main__':
    main()
