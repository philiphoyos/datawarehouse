import pandas as pd
import uuid
from etl.extract import extract_data
# from etl.transform import transform_data
from etl.load import load_data

def update_dimensions(source, destination):
    new_data = extract_data(source)
    existing_data = pd.read_parquet(destination)
    
    # Mark the end_date of existing records that are being updated
    for _, new_row in new_data.iterrows():
        mask = (existing_data['product_id'] == new_row['product_id']) & (existing_data['end_date'].isna())
        existing_data.loc[mask, 'end_date'] = new_row['start_date'] - pd.Timedelta(days=1)
    
    # Assign surrogate keys to new records
    new_data['surrogate_key'] = [str(uuid.uuid4()) for _ in range(len(new_data))]
    
    # Concatenate new records
    updated_data = pd.concat([existing_data, new_data])
    
    load_data(updated_data, destination)
