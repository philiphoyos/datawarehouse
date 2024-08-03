import pandas as pd
from etl.extract import extract_data
from etl.transform.transform_customers import transform_customers
from etl.load import load_data

if __name__ == '__main__':
    # Read staged data
    raw_data = pd.read_parquet('staging/customers.parquet')
    
    # Transform data
    transformed_data = transform_customers(raw_data)
    
    # Load transformed data into the data warehouse
    load_data(transformed_data, 'warehouse/customer_dim.parquet')
