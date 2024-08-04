from etl.extract import extract_data, stage_data
# from ..etl.extract import extract_data, stage_data

if __name__ == '__main__':
    # Extract products data from a CSV file and stage it as a Parquet file
    products_data = extract_data('csv', 'raw_data/products.csv')
    stage_data(products_data, 'staging/products.parquet')
    
    # Extract customers data from a CSV file and stage it as a Parquet file
    customers_data = extract_data('csv', 'raw_data/customers.csv')
    stage_data(customers_data, 'staging/customers.parquet')
    
    # Extract sales data from a CSV file and stage it as a Parquet file
    sales_data = extract_data('csv', 'raw_data/sales.csv')
    stage_data(sales_data, 'staging/sales.parquet')
