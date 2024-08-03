# import pandas as pd
# from etl.extract import extract_data
# from etl.transform.transform_products import transform_products
# from etl.load import load_data

# if __name__ == '__main__':
#     # Read staged data
#     raw_data = pd.read_parquet('staging/products.parquet')
    
#     # Transform data
#     transformed_data = transform_products(raw_data)
    
#     # Load transformed data into the data warehouse
#     load_data(transformed_data, 'warehouse/product_dim.parquet')

import pandas as pd
from etl.extract import extract_data, stage_data
from etl.transform.transform_products import transform_products
from etl.load import load_data
import datetime

if __name__ == '__main__':
    # Define the last extraction date (this would typically come from a metadata store or config)
    last_extract_date = datetime.datetime(2023, 6, 2)
    
    # Extract new or updated products data
    new_products_data = extract_data('raw_data/products.csv', last_extract_date)
    stage_data(new_products_data, 'staging/products.parquet')
    
    # Load existing dimension data
    try:
        existing_products_data = pd.read_parquet('warehouse/product_dim.parquet')
    except FileNotFoundError:
        existing_products_data = pd.DataFrame()
    
    # Transform new or updated products data
    transformed_products_data = transform_products(new_products_data, existing_products_data)
    
    # Load transformed data into the data warehouse
    load_data(transformed_products_data, 'warehouse/product_dim.parquet')
