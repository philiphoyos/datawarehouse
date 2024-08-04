import pandas as pd

# def transform_sales(data, product_data, customer_data):
#     data = data.merge(product_data[['product_id', 'surrogate_key']], on='product_id', how='left')
#     data = data.rename(columns={'surrogate_key': 'product_sk'})
    
#     data = data.merge(customer_data[['customer_id', 'surrogate_key']], on='customer_id', how='left')
#     data = data.rename(columns={'surrogate_key': 'customer_sk'})
    
#     data = data.drop(columns=['product_id', 'customer_id'])
#     data['sale_date'] = pd.to_datetime(data['sale_date'])
#     data['updated_at'] = pd.to_datetime(data['updated_at'])
#     return data


import pandas as pd

def transform_sales(new_data, existing_data):
    # Convert date columns to datetime
    new_data['sale_date'] = pd.to_datetime(new_data['sale_date'])
    existing_data['sale_date'] = pd.to_datetime(existing_data['sale_date'])

    # Merge new data with existing data
    combined_data = pd.concat([existing_data, new_data], ignore_index=True)

    # Sort by sale_date and remove duplicates (keeping the latest records)
    combined_data.sort_values(by=['sale_date'], inplace=True)
    combined_data.drop_duplicates(subset=['sale_id'], keep='last', inplace=True)

    return combined_data
