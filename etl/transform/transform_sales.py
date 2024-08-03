import pandas as pd

def transform_sales(data, product_data, customer_data):
    data = data.merge(product_data[['product_id', 'surrogate_key']], on='product_id', how='left')
    data = data.rename(columns={'surrogate_key': 'product_sk'})
    
    data = data.merge(customer_data[['customer_id', 'surrogate_key']], on='customer_id', how='left')
    data = data.rename(columns={'surrogate_key': 'customer_sk'})
    
    data = data.drop(columns=['product_id', 'customer_id'])
    data['sale_date'] = pd.to_datetime(data['sale_date'])
    data['updated_at'] = pd.to_datetime(data['updated_at'])
    return data
