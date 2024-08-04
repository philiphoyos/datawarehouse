import pandas as pd
import uuid

# def transform_products(data):
#     data['surrogate_key'] = [str(uuid.uuid4()) for _ in range(len(data))]
#     data['start_date'] = pd.to_datetime('today')
#     data['end_date'] = pd.NaT
#     return data

def transform_products(new_data, existing_data=None):
    if existing_data == None:
        new_data['surrogate_key'] = [str(uuid.uuid4()) for _ in range(len(new_data))]
        new_data['start_date'] = pd.to_datetime('today')
        new_data['end_date'] = pd.NaT

    else:
        new_data['surrogate_key'] = new_data.apply(
            lambda row: existing_data.loc[existing_data['product_id'] == row['product_id'], 'surrogate_key'].values[0]
            if row['product_id'] in existing_data['product_id'].values
            else str(uuid.uuid4()), axis=1
        )
        new_data['start_date'] = pd.to_datetime('today')
        new_data['end_date'] = pd.NaT
    return new_data